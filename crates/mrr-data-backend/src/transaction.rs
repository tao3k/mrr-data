//! Shared logical metadata engine, independent of every database and query dialect.
use crate::scheme_record::{AuthorityCompletion, Completion, Record, decode, encode};
use crate::{
    AuthorityChange, AuthorityKey, AuthorityState, AuthorityStatus, BackendError, MetadataProvider,
    ProviderCapabilities, PublicationDelivery, StoredOutcome, StoredRevision, StoredWrite,
    providers::{MetadataTransaction, ProviderResult, TransactionProvider},
};
use mrr_data_content::{
    ConditionalCommitDisposition, ConditionalCommitPortError as PortError,
    ConditionalContentReceipt, ContentRevision, PublishReceipt,
};
fn before(e: BackendError) -> PortError<BackendError, ()> {
    PortError::BeforeCommit(e)
}
fn home(write: &StoredWrite) -> [&str; 3] {
    [&write.profile, &write.namespace, &write.scope]
}
fn authority_home(authority: &AuthorityKey) -> [&str; 3] {
    [&authority.profile, &authority.namespace, &authority.scope]
}
fn get<T: Record>(tx: &mut dyn MetadataTransaction, key: &str) -> ProviderResult<Option<T>> {
    tx.get(key)
        .map_err(before)?
        .as_deref()
        .map(decode)
        .transpose()
        .map_err(before)
}
fn put<T: Record>(tx: &mut dyn MetadataTransaction, key: &str, value: &T) -> ProviderResult<()> {
    tx.put(key, &encode(value).map_err(before)?).map_err(before)
}
fn recover(write: &StoredWrite, bytes: Option<&[u8]>) -> ProviderResult<Option<StoredRevision>> {
    let Some(bytes) = bytes else {
        write
            .content_write()
            .recover_receipt(None)
            .map_err(PortError::Protocol)?;
        return Ok(None);
    };
    let stored: Completion = decode(bytes).map_err(before)?;
    if stored.write != *write {
        return Err(PortError::Protocol(
            mrr_data_content::ConditionalCommitError::OperationConflict,
        ));
    }
    let receipt = ConditionalContentReceipt {
        write: stored.write.content_write(),
        committed: stored.committed.into(),
    };
    write
        .content_write()
        .recover_receipt(Some(&receipt))
        .map_err(PortError::Protocol)?;
    Ok(Some(stored.committed))
}
fn validate_authorities(
    tx: &mut dyn MetadataTransaction,
    write: &StoredWrite,
) -> ProviderResult<()> {
    let required: Option<Vec<String>> = get(tx, &key("authority-set", &home(write))?)?;
    let created: Option<bool> = get(tx, &key("authority-set-created", &home(write))?)?;
    if created == Some(true) && required.is_none() {
        return Err(before(BackendError::Corrupt));
    }
    let required = required.unwrap_or_default();
    if required.len() > 16 || required.len() != write.authorities.len() {
        return Err(before(BackendError::AuthorityConflict));
    }
    for (id, expected) in required.iter().zip(&write.authorities) {
        if id != &expected.authority_id {
            return Err(before(BackendError::AuthorityConflict));
        }
        let current: AuthorityState = get(
            tx,
            &key(
                "authority",
                &[&write.profile, &write.namespace, &write.scope, id],
            )?,
        )?
        .ok_or_else(|| before(BackendError::Corrupt))?;
        if current.status == AuthorityStatus::Retired {
            return Err(before(BackendError::AuthorityRetired));
        }
        if current != expected.state {
            return Err(before(BackendError::AuthorityConflict));
        }
    }
    Ok(())
}
fn commit(
    tx: &mut dyn MetadataTransaction,
    write: &StoredWrite,
    physical: Option<&PublishReceipt>,
    validate: &mut dyn FnMut(Option<ContentRevision>) -> bool,
) -> ProviderResult<StoredOutcome> {
    let op_key = key(
        "operation",
        &[
            &write.profile,
            &write.namespace,
            &write.scope,
            &write.operation_id,
        ],
    )?;
    if let Some(committed) = recover(write, tx.get(&op_key).map_err(before)?.as_deref())? {
        require_delivery(tx, write, committed)?;
        return Ok(StoredOutcome {
            committed,
            replayed: true,
        });
    }
    let head_key = key("head", &home(write))?;
    let current: Option<StoredRevision> = get(tx, &head_key)?;
    let head_created: Option<bool> = get(tx, &key("head-created", &home(write))?)?;
    if current.is_some() != (head_created == Some(true)) {
        return Err(before(BackendError::Corrupt));
    }
    validate_selected(tx, write, current)?;
    let ConditionalCommitDisposition::Apply(next) = write
        .content_write()
        .decide_commit(current.map(Into::into), physical, None)
        .map_err(PortError::Protocol)?
    else {
        return Err(before(BackendError::Corrupt));
    };
    validate_authorities(tx, write)?;
    if !validate(current.map(Into::into)) {
        return Err(PortError::Validation(()));
    }
    seal_home(tx, &home(write))?;
    let committed = StoredRevision::from(next);
    put(
        tx,
        &op_key,
        &Completion {
            write: write.clone(),
            committed,
        },
    )?;
    put(
        tx,
        &delivery_key(write, committed.revision)?,
        &PublicationDelivery {
            write: write.clone(),
            committed,
            acknowledged: false,
        },
    )?;
    put(tx, &head_key, &committed)?;
    put(tx, &key("head-created", &home(write))?, &true)?;
    Ok(StoredOutcome {
        committed,
        replayed: false,
    })
}
fn advance(
    tx: &mut dyn MetadataTransaction,
    change: &AuthorityChange,
) -> ProviderResult<AuthorityState> {
    let next = change.next().map_err(before)?;
    let generation = next.generation.to_string();
    let a = &change.key;
    let history = key(
        "authority-operation",
        &[
            &a.profile,
            &a.namespace,
            &a.scope,
            &a.authority_id,
            &generation,
        ],
    )?;
    if let Some(old) = get::<AuthorityCompletion>(tx, &history)? {
        if old.change != *change || old.committed != next {
            return Err(before(BackendError::AuthorityConflict));
        }
        return Ok(old.committed);
    }
    let state_key = key(
        "authority",
        &[&a.profile, &a.namespace, &a.scope, &a.authority_id],
    )?;
    let current: Option<AuthorityState> = get(tx, &state_key)?;
    let set_key = key("authority-set", &authority_home(a))?;
    let required: Option<Vec<String>> = get(tx, &set_key)?;
    let created: Option<bool> = get(tx, &key("authority-set-created", &authority_home(a))?)?;
    if created == Some(true) && required.is_none() {
        return Err(before(BackendError::Corrupt));
    }
    let mut required = required.unwrap_or_default();
    if current.is_none() && required.contains(&a.authority_id) {
        return Err(before(BackendError::Corrupt));
    }
    if current != change.proposal.expected {
        return Err(before(BackendError::AuthorityConflict));
    }
    seal_home(tx, &authority_home(a))?;
    if current.is_none() {
        if required.len() >= 16 {
            return Err(before(BackendError::Limit));
        }
        required.push(a.authority_id.clone());
        required.sort();
        put(tx, &set_key, &required)?;
        put(
            tx,
            &key("authority-set-created", &authority_home(a))?,
            &true,
        )?;
    }
    put(
        tx,
        &history,
        &AuthorityCompletion {
            change: change.clone(),
            committed: next,
        },
    )?;
    put(tx, &state_key, &next)?;
    Ok(next)
}
impl<P: TransactionProvider> MetadataProvider for P {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            atomic_head_operation: true,
            durable_commit: true,
            historical_lookup: true,
            authority_versions: crate::AuthorityCapability::Transactional,
        }
    }
    fn open(&self) -> Result<(), BackendError> {
        self.open_storage()
    }
    fn close(&self) -> Result<(), BackendError> {
        self.close_storage()
    }
    fn commit(
        &self,
        write: &StoredWrite,
        physical: Option<&PublishReceipt>,
        validate: &mut dyn FnMut(Option<ContentRevision>) -> bool,
    ) -> ProviderResult<StoredOutcome> {
        let mut result = None;
        self.transaction(&mut |tx| {
            result = Some(commit(tx, write, physical, validate)?);
            Ok(())
        })?;
        result.ok_or_else(|| before(BackendError::Corrupt))
    }
    fn recover(&self, write: &StoredWrite) -> ProviderResult<Option<StoredRevision>> {
        let bytes = self
            .read(&key(
                "operation",
                &[
                    &write.profile,
                    &write.namespace,
                    &write.scope,
                    &write.operation_id,
                ],
            )?)
            .map_err(before)?;
        let committed = recover(write, bytes.as_deref())?;
        if let Some(revision) = committed {
            let row = self
                .publication_delivery(
                    &[
                        write.profile.clone(),
                        write.namespace.clone(),
                        write.scope.clone(),
                    ],
                    revision.revision,
                )?
                .ok_or_else(|| before(BackendError::Corrupt))?;
            if row.write != *write || row.committed != revision {
                return Err(before(BackendError::Corrupt));
            }
        }
        Ok(committed)
    }
    fn publication_delivery(
        &self,
        home: &[String; 3],
        revision: u64,
    ) -> ProviderResult<Option<PublicationDelivery>> {
        let parts = [&*home[0], &*home[1], &*home[2], &revision.to_string()];
        let Some(bytes) = self
            .read(&key("publication-delivery", &parts)?)
            .map_err(before)?
        else {
            return Ok(None);
        };
        let row: PublicationDelivery = decode(&bytes).map_err(before)?;
        if [&row.write.profile, &row.write.namespace, &row.write.scope]
            != [&home[0], &home[1], &home[2]]
            || row.committed.revision != revision
        {
            return Err(before(BackendError::Corrupt));
        }
        let op = self.read(&operation_key(&row.write)?).map_err(before)?;
        if recover(&row.write, op.as_deref())? != Some(row.committed) {
            return Err(before(BackendError::Corrupt));
        }
        Ok(Some(row))
    }
    fn acknowledge_publication(&self, row: &PublicationDelivery) -> ProviderResult<()> {
        self.transaction(&mut |tx| {
            let mut stored = require_delivery(tx, &row.write, row.committed)?;
            let op = tx.get(&operation_key(&row.write)?).map_err(before)?;
            if recover(&row.write, op.as_deref())? != Some(row.committed) {
                return Err(before(BackendError::Corrupt));
            }
            if !stored.acknowledged {
                seal_home(tx, &home(&row.write))?;
                stored.acknowledged = true;
                put(
                    tx,
                    &delivery_key(&row.write, row.committed.revision)?,
                    &stored,
                )?;
            }
            Ok(())
        })
    }
    fn authority(&self, a: &AuthorityKey) -> ProviderResult<Option<AuthorityState>> {
        self.read(&key(
            "authority",
            &[&a.profile, &a.namespace, &a.scope, &a.authority_id],
        )?)
        .map_err(before)?
        .as_deref()
        .map(decode)
        .transpose()
        .map_err(before)
    }
    fn advance_authority(&self, change: &AuthorityChange) -> ProviderResult<AuthorityState> {
        let mut result = None;
        self.transaction(&mut |tx| {
            result = Some(advance(tx, change)?);
            Ok(())
        })?;
        result.ok_or_else(|| before(BackendError::Corrupt))
    }
}

fn seal_home(tx: &mut dyn MetadataTransaction, parts: &[&str]) -> ProviderResult<()> {
    // Every fresh content/authority transaction writes the same home sequence.
    // Native write-conflict detection prevents snapshot-isolation write skew
    // between a permission update and a content write that only read that row.
    let k = key("home-sequence", parts)?;
    let current: u64 = get(tx, &k)?.unwrap_or(0);
    put(
        tx,
        &k,
        &current
            .checked_add(1)
            .ok_or_else(|| before(BackendError::Limit))?,
    )
}

fn key(kind: &str, parts: &[&str]) -> ProviderResult<String> {
    crate::scheme_record::key(kind, parts).map_err(before)
}

fn operation_key(w: &StoredWrite) -> ProviderResult<String> {
    key(
        "operation",
        &[&w.profile, &w.namespace, &w.scope, &w.operation_id],
    )
}
fn delivery_key(w: &StoredWrite, revision: u64) -> ProviderResult<String> {
    key(
        "publication-delivery",
        &[&w.profile, &w.namespace, &w.scope, &revision.to_string()],
    )
}
fn require_delivery(
    tx: &mut dyn MetadataTransaction,
    write: &StoredWrite,
    committed: StoredRevision,
) -> ProviderResult<PublicationDelivery> {
    let row: PublicationDelivery = get(tx, &delivery_key(write, committed.revision)?)?
        .ok_or_else(|| before(BackendError::Corrupt))?;
    if row.write != *write || row.committed != committed {
        return Err(before(BackendError::Corrupt));
    }
    Ok(row)
}

fn validate_selected(
    tx: &mut dyn MetadataTransaction,
    write: &StoredWrite,
    current: Option<StoredRevision>,
) -> ProviderResult<()> {
    if let Some(selected) = current {
        let delivery: PublicationDelivery = get(tx, &delivery_key(write, selected.revision)?)?
            .ok_or_else(|| before(BackendError::Corrupt))?;
        if home(&delivery.write) != home(write) || delivery.committed != selected {
            return Err(before(BackendError::Corrupt));
        }
        let operation = tx.get(&operation_key(&delivery.write)?).map_err(before)?;
        if recover(&delivery.write, operation.as_deref())? != Some(selected) {
            return Err(before(BackendError::Corrupt));
        }
    }
    Ok(())
}
