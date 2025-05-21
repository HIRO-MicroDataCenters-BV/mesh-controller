use p2panda_core::Hash;
use p2panda_core::PublicKey;
use p2panda_net::TopicId;
use p2panda_sync::TopicQuery;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Query {
    EMDC { public_key: PublicKey },
    Resources { public_key: PublicKey },
    NoSyncEMDC { public_key: PublicKey },
    NoSyncResources { public_key: PublicKey },
}

impl Query {
    pub fn is_no_sync(&self) -> bool {
        matches!(self, Self::NoSyncEMDC { .. } | Self::NoSyncResources { .. })
    }

    fn prefix(&self) -> &str {
        match self {
            Self::EMDC { .. } => "emdc",
            Self::Resources { .. } => "resources",
            Self::NoSyncEMDC { .. } => "emdc",
            Self::NoSyncResources { .. } => "resources",
        }
    }
}

impl TopicQuery for Query {}

impl TopicId for Query {
    fn id(&self) -> [u8; 32] {
        let hash = match self {
            Self::EMDC { public_key, .. } => Hash::new(format!("{}{}", self.prefix(), public_key)),
            Self::Resources { public_key, .. } => {
                Hash::new(format!("{}{}", self.prefix(), public_key))
            }
            Self::NoSyncEMDC { public_key } => {
                Hash::new(format!("{}{}", self.prefix(), public_key))
            }
            Self::NoSyncResources { public_key } => {
                Hash::new(format!("{}{}", self.prefix(), public_key))
            }
        };

        *hash.as_bytes()
    }
}

impl std::fmt::Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Query::EMDC { public_key } => {
                write!(f, "EMDC public_key=\"{}\"", {
                    let mut public_key_str = public_key.to_string();
                    public_key_str.truncate(6);
                    public_key_str
                })
            }
            Query::Resources { public_key, .. } => {
                write!(f, "Resources public_key=\"{}\"", {
                    let mut public_key_str = public_key.to_string();
                    public_key_str.truncate(6);
                    public_key_str
                })
            }
            Query::NoSyncEMDC { .. } => write!(f, "EMDC no-sync"),
            Query::NoSyncResources { .. } => write!(f, "Resources no-sync"),
        }
    }
}
