use crate::server::handler::model::{SetReq, SetRes};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};

/// A request to the KV store.
#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub enum Request {
    Set(SetReq),
}
impl Request {
    pub fn set(key: impl Into<String>, value: impl Into<String>) -> Self {
        Request::Set(SetReq {
            key: key.into(),
            value: Vec::from(value.into()),
            ex_time: 100000,
        })
    }
    pub fn hash_code(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Request::Set(req) => write!(f, "Set: {}", req),
        }
    }
}

/// A response from the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Set(SetRes),
    Null,
}

impl Response {
    pub fn none() -> Self {
        Response::Null
    }
}
