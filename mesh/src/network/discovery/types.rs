use std::collections::BTreeMap;

use p2panda_core::PublicKey;

use crate::{mesh::topic::InstanceId, network::discovery::nodes::MembershipState};

pub type Timestamp = u64;

#[derive(Debug, Clone)]
pub struct Membership {
    timestamp: Timestamp,
    instances: BTreeMap<String, InstanceId>,
}

impl Default for Membership {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Membership {
    pub fn new(timestamp: Timestamp) -> Membership {
        Membership {
            instances: BTreeMap::new(),
            timestamp,
        }
    }

    pub fn add(&mut self, instance: InstanceId) {
        self.instances.insert(instance.zone.to_owned(), instance);
    }

    pub fn get_timestamp(&self) -> Timestamp {
        self.timestamp
    }

    pub fn get_instance(&self, zone: &str) -> Option<&InstanceId> {
        self.instances.get(zone)
    }

    pub fn default_owner(&self) -> Option<&InstanceId> {
        let mut instances: Vec<&InstanceId> = self.instances.values().collect();
        instances.sort_by_key(|inst| inst.start_time);
        instances.first().copied()
    }

    pub fn is_equal(&self, other: &Membership) -> bool {
        self.instances == other.instances
    }
}

impl std::fmt::Display for Membership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Membership{{")?;
        let mut sep = false;
        for instance in self.instances.values() {
            if sep {
                write!(f, ", ")?;
            }
            write!(f, "{}@{}", instance.zone, instance.start_time)?;
            sep = true;
        }
        write!(f, "}}")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MembershipUpdate {
    pub membership: Membership,
    pub peers: Vec<PeerStateUpdate>,
}

#[derive(Debug, Clone)]
pub struct PeerStateUpdate {
    pub peer: PublicKey,
    pub state: MembershipState,
    pub instance: Option<InstanceId>,
    pub timestamp: u64,
}

#[cfg(test)]
pub mod tests {

    use super::*;

    #[test]
    pub fn get_default_owner() {
        let mut membership = Membership::default();
        assert_eq!(membership.default_owner(), None);

        let i1 = InstanceId {
            zone: "z1".into(),
            start_time: 5,
        };
        let i2 = InstanceId {
            zone: "z2".into(),
            start_time: 4,
        };
        let i3 = InstanceId {
            zone: "z2".into(),
            start_time: 3,
        };

        let i2_2 = InstanceId {
            zone: "z2".into(),
            start_time: 2,
        };

        membership.add(i1.to_owned());
        assert_eq!(membership.default_owner(), Some(&i1));

        membership.add(i2.to_owned());
        assert_eq!(membership.default_owner(), Some(&i2));

        membership.add(i3.to_owned());
        assert_eq!(membership.default_owner(), Some(&i3));

        membership.add(i2_2.to_owned());
        assert_eq!(membership.default_owner(), Some(&i2_2));
    }
}
