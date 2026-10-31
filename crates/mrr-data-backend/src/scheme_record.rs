//! Bounded inert Scheme records with separately admitted Schema identities.
use crate::{
    AuthorityChange, AuthorityExpectation, AuthorityKey, AuthorityProposal, AuthorityState,
    AuthorityStatus, BackendError, PublicationDelivery, StoredRevision, StoredWrite,
};
use cid::Cid;
use mrr_data_profile as schema;
use std::fmt::Write;
const LIMIT: usize = 65_536;
#[derive(Debug)]
pub(crate) enum Datum {
    Text(String),
    Natural(u64),
    Boolean(bool),
    List(Vec<Datum>),
}
pub(crate) trait Record: Sized {
    fn datum(&self) -> Datum;
    fn budget(&self, remaining: &mut usize) -> Result<(), BackendError> {
        charge(&self.datum(), remaining)
    }
    fn read(value: Datum) -> Result<Self, BackendError>;
}
fn corrupt<T>() -> Result<T, BackendError> {
    Err(BackendError::Corrupt)
}
fn spend(remaining: &mut usize, amount: usize) -> Result<(), BackendError> {
    *remaining = remaining.checked_sub(amount).ok_or(BackendError::Limit)?;
    Ok(())
}
fn text_budget(value: &str, remaining: &mut usize) -> Result<(), BackendError> {
    spend(remaining, 2)?;
    for ch in value.chars() {
        spend(
            remaining,
            match ch {
                '"' | '\\' => 2,
                ch if ch.is_control() => format!("\\x{:x};", ch as u32).len(),
                ch => ch.len_utf8(),
            },
        )?;
    }
    Ok(())
}
fn charge(value: &Datum, remaining: &mut usize) -> Result<(), BackendError> {
    match value {
        Datum::Text(s) => text_budget(s, remaining),
        Datum::Natural(n) => spend(remaining, n.to_string().len()),
        Datum::Boolean(_) => spend(remaining, 2),
        Datum::List(values) => {
            spend(remaining, 2 + values.len().saturating_sub(1))?;
            for value in values {
                charge(value, remaining)?;
            }
            Ok(())
        }
    }
}
fn emit(value: &Datum, out: &mut String) {
    match value {
        Datum::Text(value) => {
            out.push('"');
            for ch in value.chars() {
                match ch {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    ch if ch.is_control() => {
                        write!(out, "\\x{:x};", u32::from(ch))
                            .expect("String formatting cannot fail");
                    }
                    ch => out.push(ch),
                }
            }
            out.push('"');
        }
        Datum::Natural(value) => out.push_str(&value.to_string()),
        Datum::Boolean(value) => out.push_str(if *value { "#t" } else { "#f" }),
        Datum::List(values) => {
            out.push('(');
            for (i, value) in values.iter().enumerate() {
                if i != 0 {
                    out.push(' ');
                }
                emit(value, out);
            }
            out.push(')');
        }
    }
}
pub(crate) fn encode<T: Record>(value: &T) -> Result<Vec<u8>, BackendError> {
    let mut remaining = LIMIT;
    value.budget(&mut remaining)?;
    let mut text = String::with_capacity(LIMIT - remaining);
    emit(&value.datum(), &mut text);
    if text.len() > LIMIT {
        return Err(BackendError::Limit);
    }
    Ok(text.into_bytes())
}
#[cfg(any(feature = "turso", feature = "duckdb", test))]
pub(crate) fn schema_marker(schema: schema::SchemaIdentity) -> Vec<u8> {
    let mut text = String::new();
    emit(
        &Datum::List(vec![
            Datum::Text(schema.namespace.into()),
            Datum::Natural(schema.version),
        ]),
        &mut text,
    );
    text.into_bytes()
}
pub(crate) fn key(kind: &str, parts: &[&str]) -> Result<String, BackendError> {
    let mut remaining = 8192;
    spend(&mut remaining, 7 + parts.len().saturating_sub(1))?;
    spend(
        &mut remaining,
        schema::BACKEND_KEY_SCHEMA.version.to_string().len(),
    )?;
    text_budget(schema::BACKEND_KEY_SCHEMA.namespace, &mut remaining)?;
    text_budget(kind, &mut remaining)?;
    for part in parts {
        text_budget(part, &mut remaining)?;
    }
    let value = Datum::List(vec![
        Datum::Text(schema::BACKEND_KEY_SCHEMA.namespace.into()),
        Datum::Natural(schema::BACKEND_KEY_SCHEMA.version),
        Datum::Text(kind.into()),
        Datum::List(parts.iter().map(|s| Datum::Text((*s).into())).collect()),
    ]);
    let mut text = String::new();
    emit(&value, &mut text);
    Ok(text)
}
struct Reader<'a> {
    input: &'a str,
    offset: usize,
}
impl Reader<'_> {
    fn peek(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }
    fn take(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.offset += ch.len_utf8();
        Some(ch)
    }
    fn space(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.take();
        }
    }
    fn datum(&mut self, depth: usize) -> Result<Datum, BackendError> {
        if depth > 32 {
            return corrupt();
        }
        self.space();
        match self.take() {
            Some('(') => self.list(depth),
            Some('"') => self.string(),
            Some('#') => self.boolean(),
            Some(ch) if ch.is_ascii_digit() => self.natural(self.offset - 1),
            _ => corrupt(),
        }
    }
    fn list(&mut self, depth: usize) -> Result<Datum, BackendError> {
        let mut values = Vec::new();
        loop {
            self.space();
            if self.peek() == Some(')') {
                self.take();
                return Ok(Datum::List(values));
            }
            if self.peek().is_none() || values.len() >= 16 {
                return corrupt();
            }
            values.push(self.datum(depth + 1)?);
        }
    }
    fn string(&mut self) -> Result<Datum, BackendError> {
        let mut value = String::new();
        loop {
            match self.take() {
                Some('"') => return Ok(Datum::Text(value)),
                Some('\\') => value.push(self.escape()?),
                Some(ch) if !ch.is_control() => value.push(ch),
                _ => return corrupt(),
            }
        }
    }
    fn escape(&mut self) -> Result<char, BackendError> {
        match self.take() {
            Some('"') => Ok('"'),
            Some('\\') => Ok('\\'),
            Some('x') => self.scalar(),
            _ => corrupt(),
        }
    }
    fn scalar(&mut self) -> Result<char, BackendError> {
        let begin = self.offset;
        while self.peek().is_some_and(|ch| ch.is_ascii_hexdigit()) {
            self.take();
        }
        let digits = &self.input[begin..self.offset];
        if digits.is_empty() || digits.len() > 6 || self.take() != Some(';') {
            return corrupt();
        }
        let scalar = u32::from_str_radix(digits, 16).map_err(|_| BackendError::Corrupt)?;
        char::from_u32(scalar).ok_or(BackendError::Corrupt)
    }
    fn delimiter(&self) -> bool {
        self.peek().is_none_or(|ch| ch.is_whitespace() || ch == ')')
    }
    fn boolean(&mut self) -> Result<Datum, BackendError> {
        let value = match self.take() {
            Some('t') => true,
            Some('f') => false,
            _ => return corrupt(),
        };
        if !self.delimiter() {
            return corrupt();
        }
        Ok(Datum::Boolean(value))
    }
    fn natural(&mut self, begin: usize) -> Result<Datum, BackendError> {
        while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            self.take();
        }
        let number = &self.input[begin..self.offset];
        if !self.delimiter() || (number.len() > 1 && number.starts_with('0')) {
            return corrupt();
        }
        Ok(Datum::Natural(
            number.parse().map_err(|_| BackendError::Corrupt)?,
        ))
    }
}
pub(crate) fn decode<T: Record>(bytes: &[u8]) -> Result<T, BackendError> {
    if bytes.len() > LIMIT {
        return corrupt();
    }
    let input = std::str::from_utf8(bytes).map_err(|_| BackendError::Corrupt)?;
    let mut reader = Reader { input, offset: 0 };
    let value = reader.datum(0)?;
    reader.space();
    if reader.offset != input.len() {
        return corrupt();
    }
    T::read(value)
}
impl Record for String {
    fn budget(&self, remaining: &mut usize) -> Result<(), BackendError> {
        text_budget(self, remaining)
    }
    fn datum(&self) -> Datum {
        Datum::Text(self.clone())
    }
    fn read(value: Datum) -> Result<Self, BackendError> {
        if let Datum::Text(s) = value {
            Ok(s)
        } else {
            corrupt()
        }
    }
}
impl Record for u64 {
    fn datum(&self) -> Datum {
        Datum::Natural(*self)
    }
    fn read(value: Datum) -> Result<Self, BackendError> {
        if let Datum::Natural(n) = value {
            Ok(n)
        } else {
            corrupt()
        }
    }
}
impl Record for bool {
    fn datum(&self) -> Datum {
        Datum::Boolean(*self)
    }
    fn read(value: Datum) -> Result<Self, BackendError> {
        if let Datum::Boolean(b) = value {
            Ok(b)
        } else {
            corrupt()
        }
    }
}
impl Record for Cid {
    fn datum(&self) -> Datum {
        Datum::Text(self.to_string())
    }
    fn read(value: Datum) -> Result<Self, BackendError> {
        String::read(value)?
            .parse()
            .map_err(|_| BackendError::Corrupt)
    }
}
impl<T: Record> Record for Option<T> {
    fn budget(&self, remaining: &mut usize) -> Result<(), BackendError> {
        self.as_ref()
            .map_or_else(|| Ok(()), |v| v.budget(remaining))?;
        if self.is_none() {
            spend(remaining, 2)?;
        }
        Ok(())
    }
    fn datum(&self) -> Datum {
        self.as_ref().map_or(Datum::Boolean(false), Record::datum)
    }
    fn read(value: Datum) -> Result<Self, BackendError> {
        if matches!(value, Datum::Boolean(false)) {
            Ok(None)
        } else {
            T::read(value).map(Some)
        }
    }
}
impl<T: Record> Record for Vec<T> {
    fn budget(&self, remaining: &mut usize) -> Result<(), BackendError> {
        if self.len() > 16 {
            return Err(BackendError::Limit);
        }
        spend(remaining, 2 + self.len().saturating_sub(1))?;
        for value in self {
            value.budget(remaining)?;
        }
        Ok(())
    }
    fn datum(&self) -> Datum {
        Datum::List(self.iter().map(Record::datum).collect())
    }
    fn read(value: Datum) -> Result<Self, BackendError> {
        if let Datum::List(values) = value {
            if values.len() > 16 {
                return corrupt();
            }
            values.into_iter().map(T::read).collect()
        } else {
            corrupt()
        }
    }
}
impl Record for AuthorityStatus {
    fn datum(&self) -> Datum {
        Datum::Text(
            match self {
                Self::Active => "active",
                Self::Retired => "retired",
            }
            .into(),
        )
    }
    fn read(value: Datum) -> Result<Self, BackendError> {
        match String::read(value)?.as_str() {
            "active" => Ok(Self::Active),
            "retired" => Ok(Self::Retired),
            _ => corrupt(),
        }
    }
}
macro_rules! record {
    ($ty:ty, $schema:expr, {$($name:ident:$field:ty),* $(,)?}) => {
        impl Record for $ty {
            fn budget(&self, remaining:&mut usize)->Result<(),BackendError> {
                spend(remaining,3+[$(stringify!($name)),*].len())?;
                $schema.version.budget(remaining)?;
                text_budget($schema.namespace,remaining)?; $(self.$name.budget(remaining)?;)* Ok(()) }
            fn datum(&self)->Datum { Datum::List(vec![Datum::Text($schema.namespace.into()),Datum::Natural($schema.version),$(self.$name.datum()),*]) }
            fn read(value:Datum)->Result<Self,BackendError> {
                let Datum::List(values)=value else { return corrupt(); };
                if values.len()!=2+[$(stringify!($name)),*].len() { return corrupt(); }
                let mut values=values.into_iter();
                let namespace = String::read(values.next().ok_or(BackendError::Corrupt)?)?;
                let version = u64::read(values.next().ok_or(BackendError::Corrupt)?)?;
                if !$schema.accepts(&namespace,version) { return corrupt(); }
                Ok(Self { $($name: <$field>::read(values.next().ok_or(BackendError::Corrupt)?)?),* })
            }
        }
    }
}
pub(crate) struct Completion {
    pub write: StoredWrite,
    pub committed: StoredRevision,
}
pub(crate) struct AuthorityCompletion {
    pub change: AuthorityChange,
    pub committed: AuthorityState,
}
record!(StoredRevision,schema::BACKEND_REVISION_SCHEMA,{revision:u64,root:Cid});
record!(AuthorityState,schema::BACKEND_AUTHORITY_SCHEMA,{generation:u64,commitment:Cid,status:AuthorityStatus});
record!(AuthorityExpectation,schema::BACKEND_EXPECTATION_SCHEMA,{authority_id:String,state:AuthorityState});
record!(StoredWrite,schema::BACKEND_WRITE_SCHEMA,{profile:String,namespace:String,scope:String,operation_id:String,expected:Option<StoredRevision>,replacement:Cid,authorities:Vec<AuthorityExpectation>});
record!(AuthorityKey,schema::BACKEND_AUTHORITY_KEY_SCHEMA,{profile:String,namespace:String,scope:String,authority_id:String});
record!(AuthorityProposal,schema::BACKEND_AUTHORITY_PROPOSAL_SCHEMA,{authority_id:String,expected:Option<AuthorityState>,replacement:Cid,status:AuthorityStatus});
record!(AuthorityChange,schema::BACKEND_AUTHORITY_CHANGE_SCHEMA,{key:AuthorityKey,proposal:AuthorityProposal});
record!(Completion,schema::BACKEND_COMPLETION_SCHEMA,{write:StoredWrite,committed:StoredRevision});
record!(AuthorityCompletion,schema::BACKEND_AUTHORITY_COMPLETION_SCHEMA,{change:AuthorityChange,committed:AuthorityState});

#[cfg(test)]
#[path = "../tests/unit/scheme_record.rs"]
mod tests;

record!(PublicationDelivery,schema::BACKEND_DELIVERY_SCHEMA,{write:StoredWrite,committed:StoredRevision,acknowledged:bool});
