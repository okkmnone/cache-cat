use std::fmt;
use std::io::Error;

use openraft::{NodeId, RaftTypeConfig, error::RPCError};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, MapAccess, Visitor},
    ser::SerializeMap,
};

use crate::network::node::TypeConfig;

pub type CoreRaftResult<T> = Result<T, CoreRaftError>;

#[derive(Debug, Deserialize, Serialize, thiserror::Error)]
pub enum CoreRaftError {
    #[error("Error: std::error::Error => {0}")]
    #[serde(with = "__serde_impl::box_std_error")]
    BoxStdError(#[from] Box<dyn std::error::Error>),

    #[error("Error: bincode2 => {0}")]
    #[serde(with = "__serde_impl::bincode2_error_kind")]
    Bincode2(#[from] Box<bincode2::ErrorKind>),

    #[error("Error: openraft RaftError => {0}")]
    OpenraftRaftError(#[from] openraft::error::RaftError<TypeConfig>),

    #[error("Error: openraft RaftError<_, ClientWriteError> => {0}")]
    OpenraftRaftErrorClientWriteError(#[from] openraft::error::RaftError<TypeConfig, openraft::error::ClientWriteError<TypeConfig>>),

    #[error("Error: openraft Fatal => {0}")]
    OpenraftFatal(#[from] openraft::error::Fatal<TypeConfig>),

    #[error("Error: openraft RPCError => {0}")]
    OpenraftRPCError(#[from] RPCError<TypeConfig>),

    #[error("Error: openraft StreamingError => {0}")]
    OpenraftStreamingError(#[from] openraft::error::StreamingError<TypeConfig>),

    #[error("Error: std io error = {0}")]
    #[serde(with = "__serde_impl::std_io_error")]
    StdIoError(#[from] std::io::Error),

    #[error("Error: rpc connection closed error")]
    RpcConnectionClosed,

    #[error("Error: Rpc waited canceled or connection closed")]
    RpcWaitedCanceledOrConnectionClosed,
}

mod __serde_impl {
    use super::*;

    pub mod bincode2_error_kind {
        use super::*;

        pub fn serialize<S>(err: &Box<bincode2::ErrorKind>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let error_type = match **err {
                bincode2::ErrorKind::Io(_) => "Io",
                bincode2::ErrorKind::InvalidUtf8Encoding(_) => "InvalidUtf8Encoding",
                bincode2::ErrorKind::InvalidBoolEncoding(_) => "InvalidBoolEncoding",
                bincode2::ErrorKind::InvalidCharEncoding => "InvalidCharEncoding",
                bincode2::ErrorKind::InvalidTagEncoding(_) => "InvalidTagEncoding",
                bincode2::ErrorKind::DeserializeAnyNotSupported => "DeserializeAnyNotSupported",
                bincode2::ErrorKind::SizeLimit => "SizeLimit",
                bincode2::ErrorKind::SizeTypeLimit => "SizeTypeLimit",
                bincode2::ErrorKind::SequenceMustHaveLength => "SequenceMustHaveLength",
                bincode2::ErrorKind::Custom(_) => "Custom",
            };
            
            let error_message = format!("{:?}", err);
            {
                use serde::ser::SerializeStruct;

                let mut state = serializer.serialize_struct("ErrorKind", 2)?;
                state.serialize_field("type", error_type)?;
                state.serialize_field("message", &error_message)?;
                state.end()
            }
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Box<bincode2::ErrorKind>, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(Deserialize)]
            struct ErrorKindHelper {
                #[serde(rename = "type")]
                type_name: String,
                message: String,
            }
            
            let helper = ErrorKindHelper::deserialize(deserializer)?;
            let error_kind = match helper.type_name.as_str() {
                "Io" => bincode2::ErrorKind::Io(std::io::Error::new(
                    std::io::ErrorKind::Other, 
                    helper.message
                )),
                "InvalidUtf8Encoding" => {
                    let utf8_error = std::str::from_utf8(&[0xFF]).unwrap_err();
                    bincode2::ErrorKind::InvalidUtf8Encoding(utf8_error)
                },
                "InvalidBoolEncoding" => bincode2::ErrorKind::InvalidBoolEncoding(0),
                "InvalidCharEncoding" => bincode2::ErrorKind::InvalidCharEncoding,
                "InvalidTagEncoding" => bincode2::ErrorKind::InvalidTagEncoding(0),
                "DeserializeAnyNotSupported" => bincode2::ErrorKind::DeserializeAnyNotSupported,
                "SizeLimit" => bincode2::ErrorKind::SizeLimit,
                "SizeTypeLimit" => bincode2::ErrorKind::SizeTypeLimit,
                "SequenceMustHaveLength" => bincode2::ErrorKind::SequenceMustHaveLength,
                _ => bincode2::ErrorKind::Custom(helper.message),
            };
            
            Ok(Box::new(error_kind))
        }
    }

    pub mod box_std_error {
        use super::*;

        pub fn serialize<S>(
            err: &Box<dyn std::error::Error>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_str(&err.to_string())
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Box<dyn std::error::Error>, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct ErrorVisitor;

            impl<'de> Visitor<'de> for ErrorVisitor {
                type Value = Box<dyn std::error::Error>;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("a string representing an error: Box<dyn std::error::Error>")
                }

                fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    Ok(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        v.to_string(),
                    )))
                }
            }

            deserializer.deserialize_str(ErrorVisitor)
        }
    }

    pub mod std_io_error {
        use super::*;

        pub fn serialize<S>(err: &std::io::Error, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_str(&err.to_string())
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<std::io::Error, D::Error>
        where
            D: Deserializer<'de>,
        {
            let s = String::deserialize(deserializer)?;
            Ok(std::io::Error::new(std::io::ErrorKind::Other, s))
        }
    }
}
