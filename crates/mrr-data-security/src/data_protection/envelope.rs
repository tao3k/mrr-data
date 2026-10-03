//! Randomized authenticated envelope for one verified inner content block.
//!
//! An envelope carries only a format tag, nonce and ciphertext. The Host keeps
//! the exact intent and key version in its receipt. Those fields, the inner
//! CID and the block role are authenticated as associated data. Ciphertext is
//! addressed by a fresh outer raw CID; equal plaintext is not deduplicated.

use super::{ProtectionIntent, RawStorageTier};
use cid::Cid;
use mrr_data_content::{ContentBlock, ContentCodec};
#[cfg(feature = "protected-publish")]
use ring::digest;
use ring::{
    aead,
    rand::{SecureRandom, SystemRandom},
};
use zeroize::Zeroizing;

const MAGIC: &[u8; 6] = b"MRRPE1";
const NONCE_LEN: usize = 12;
const HEADER_LEN: usize = MAGIC.len() + NONCE_LEN;
const MAX_AAD_BYTES: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtectedBlockRole {
    Child,
    Root,
    Manifest,
}

#[derive(Clone, Copy, Debug)]
pub struct ProtectedBlockBinding<'a> {
    pub intent: ProtectionIntent<'a>,
    pub inner_cid: &'a Cid,
    pub role: ProtectedBlockRole,
    pub key_version: &'a str,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ProtectedEnvelopeError {
    InvalidKey,
    InvalidProfile,
    InvalidInner,
    InvalidOuter,
    InvalidHeader,
    InvalidBinding,
    Random,
    Cryptography,
    TooLarge,
}

impl std::fmt::Display for ProtectedEnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "protected envelope: {self:?}")
    }
}

impl std::error::Error for ProtectedEnvelopeError {}

pub struct ProtectedEnvelopeKey(aead::LessSafeKey);

impl ProtectedEnvelopeKey {
    /// Construct an AES-256-GCM key from Host-controlled secret bytes.
    /// The caller owns and should clear its input key buffer.
    /// # Errors
    /// Returns `InvalidKey` unless the key has exactly 32 bytes.
    pub fn aes_256_gcm(bytes: &[u8]) -> Result<Self, ProtectedEnvelopeError> {
        let key = aead::UnboundKey::new(&aead::AES_256_GCM, bytes)
            .map_err(|_| ProtectedEnvelopeError::InvalidKey)?;
        Ok(Self(aead::LessSafeKey::new(key)))
    }
}

/// Addressed ciphertext; it contains no plaintext CID, tenant or key name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtectedBlock {
    outer_cid: Cid,
    bytes: Vec<u8>,
}

impl ProtectedBlock {
    #[must_use]
    pub const fn outer_cid(&self) -> &Cid {
        &self.outer_cid
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub fn content(&self) -> ContentBlock<'_> {
        ContentBlock::new(ContentCodec::Raw, &self.bytes)
    }
}

fn field(output: &mut Vec<u8>, value: &[u8]) -> Result<(), ProtectedEnvelopeError> {
    let length = u32::try_from(value.len()).map_err(|_| ProtectedEnvelopeError::TooLarge)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    if output.len() > MAX_AAD_BYTES {
        return Err(ProtectedEnvelopeError::TooLarge);
    }
    Ok(())
}

fn binding(
    intent: ProtectionIntent<'_>,
    inner_cid: &Cid,
    role: ProtectedBlockRole,
    key_version: &str,
) -> Result<Vec<u8>, ProtectedEnvelopeError> {
    if intent.profile != "aes-256-gcm-v1" || key_version != intent.key_version {
        return Err(ProtectedEnvelopeError::InvalidProfile);
    }
    let effect = intent.storage;
    let mut output = Vec::with_capacity(512);
    output.extend_from_slice(MAGIC);
    output.push(match role {
        ProtectedBlockRole::Child => 0,
        ProtectedBlockRole::Root => 1,
        ProtectedBlockRole::Manifest => 2,
    });
    field(&mut output, inner_cid.to_string().as_bytes())?;
    field(&mut output, effect.operation_id.as_bytes())?;
    field(&mut output, effect.subject.type_name.as_bytes())?;
    field(&mut output, effect.subject.id.as_bytes())?;
    field(&mut output, effect.purpose.as_bytes())?;
    field(&mut output, effect.snapshot_root.to_string().as_bytes())?;
    field(
        &mut output,
        effect.destination.resource.type_name.as_bytes(),
    )?;
    field(&mut output, effect.destination.resource.id.as_bytes())?;
    field(&mut output, effect.destination.tenant.as_bytes())?;
    output.push(u8::from(effect.destination.accepts_restricted));
    output.push(match effect.destination.tier {
        RawStorageTier::DurableLocal => 0,
        RawStorageTier::Remote => 1,
    });
    field(
        &mut output,
        &u32::try_from(effect.destination.accepted_owners.len())
            .map_err(|_| ProtectedEnvelopeError::TooLarge)?
            .to_be_bytes(),
    )?;
    for owner in effect.destination.accepted_owners {
        field(&mut output, owner.type_name.as_bytes())?;
        field(&mut output, owner.id.as_bytes())?;
    }
    field(
        &mut output,
        &u32::try_from(effect.sources.len())
            .map_err(|_| ProtectedEnvelopeError::TooLarge)?
            .to_be_bytes(),
    )?;
    for source in effect.sources {
        field(&mut output, source.resource.type_name.as_bytes())?;
        field(&mut output, source.resource.id.as_bytes())?;
        field(&mut output, source.owner.type_name.as_bytes())?;
        field(&mut output, source.owner.id.as_bytes())?;
        field(&mut output, source.tenant.as_bytes())?;
        output.push(u8::from(source.restricted));
    }
    field(&mut output, effect.policy_root.as_bytes())?;
    field(&mut output, effect.lineage_revision.as_bytes())?;
    field(&mut output, intent.profile.as_bytes())?;
    field(&mut output, intent.key_ref.as_bytes())?;
    field(&mut output, intent.residency.as_bytes())?;
    field(&mut output, key_version.as_bytes())?;
    if output.len() > MAX_AAD_BYTES {
        return Err(ProtectedEnvelopeError::TooLarge);
    }
    Ok(output)
}

#[cfg(feature = "protected-publish")]
pub(super) fn root_binding_digest(
    intent: ProtectionIntent<'_>,
    key_version: &str,
) -> Result<[u8; 32], ProtectedEnvelopeError> {
    let aad = binding(
        intent,
        intent.storage.snapshot_root,
        ProtectedBlockRole::Root,
        key_version,
    )?;
    digest::digest(&digest::SHA256, &aad)
        .as_ref()
        .try_into()
        .map_err(|_| ProtectedEnvelopeError::InvalidBinding)
}

/// Encrypt one inner block after verifying its exact CID. The fresh random
/// 96-bit nonce must be used under a bounded key lifecycle managed by Host.
/// # Errors
/// Returns an integrity, size, randomness, or encryption error.
pub fn seal_block(
    binding_value: ProtectedBlockBinding<'_>,
    inner_bytes: &[u8],
    key: &ProtectedEnvelopeKey,
    max_inner_bytes: usize,
) -> Result<ProtectedBlock, ProtectedEnvelopeError> {
    if inner_bytes.len() > max_inner_bytes {
        return Err(ProtectedEnvelopeError::TooLarge);
    }
    let codec = ContentCodec::from_cid(binding_value.inner_cid)
        .map_err(|_| ProtectedEnvelopeError::InvalidInner)?;
    if ContentBlock::new(codec, inner_bytes).cid() != *binding_value.inner_cid {
        return Err(ProtectedEnvelopeError::InvalidInner);
    }
    let aad = binding(
        binding_value.intent,
        binding_value.inner_cid,
        binding_value.role,
        binding_value.key_version,
    )?;
    let mut nonce = [0_u8; NONCE_LEN];
    SystemRandom::new()
        .fill(&mut nonce)
        .map_err(|_| ProtectedEnvelopeError::Random)?;
    let mut encrypted = Zeroizing::new(inner_bytes.to_vec());
    key.0
        .seal_in_place_append_tag(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(aad),
            &mut *encrypted,
        )
        .map_err(|_| ProtectedEnvelopeError::Cryptography)?;
    let mut bytes = Vec::with_capacity(HEADER_LEN + encrypted.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&nonce);
    bytes.extend_from_slice(&encrypted);
    let outer_cid = ContentBlock::new(ContentCodec::Raw, &bytes).cid();
    Ok(ProtectedBlock { outer_cid, bytes })
}

/// Verify the outer CID, authenticate the complete intent binding and decrypt
/// one block, then verify its inner CID. The returned bytes clear on drop.
/// # Errors
/// Returns an integrity, size, framing or authentication error.
pub fn open_block(
    binding_value: ProtectedBlockBinding<'_>,
    outer_cid: &Cid,
    outer_bytes: &[u8],
    key: &ProtectedEnvelopeKey,
    max_outer_bytes: usize,
) -> Result<Zeroizing<Vec<u8>>, ProtectedEnvelopeError> {
    if outer_bytes.len() > max_outer_bytes {
        return Err(ProtectedEnvelopeError::TooLarge);
    }
    if ContentBlock::new(ContentCodec::Raw, outer_bytes).cid() != *outer_cid {
        return Err(ProtectedEnvelopeError::InvalidOuter);
    }
    if outer_bytes.len() < HEADER_LEN + aead::AES_256_GCM.tag_len()
        || &outer_bytes[..MAGIC.len()] != MAGIC
    {
        return Err(ProtectedEnvelopeError::InvalidHeader);
    }
    let aad = binding(
        binding_value.intent,
        binding_value.inner_cid,
        binding_value.role,
        binding_value.key_version,
    )?;
    let nonce: [u8; NONCE_LEN] = outer_bytes[MAGIC.len()..HEADER_LEN]
        .try_into()
        .map_err(|_| ProtectedEnvelopeError::InvalidHeader)?;
    let mut plaintext = Zeroizing::new(outer_bytes[HEADER_LEN..].to_vec());
    let opened = key
        .0
        .open_in_place(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(aad),
            &mut plaintext,
        )
        .map_err(|_| ProtectedEnvelopeError::Cryptography)?;
    let length = opened.len();
    plaintext.truncate(length);
    let codec = ContentCodec::from_cid(binding_value.inner_cid)
        .map_err(|_| ProtectedEnvelopeError::InvalidInner)?;
    if ContentBlock::new(codec, &plaintext).cid() != *binding_value.inner_cid {
        return Err(ProtectedEnvelopeError::InvalidInner);
    }
    Ok(plaintext)
}
