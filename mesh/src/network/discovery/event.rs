use crate::network::discovery::types::Membership;

#[derive(Clone, Debug)]
pub enum MembershipEvent {
    Update(Membership),
}
