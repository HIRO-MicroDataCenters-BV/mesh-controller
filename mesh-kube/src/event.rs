use k8s_openapi::api::core::v1::Event;
use k8s_openapi::api::core::v1::ObjectReference;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use k8s_openapi::jiff;

#[allow(clippy::too_many_arguments)]
pub fn create_event(
    object_ref: ObjectReference,
    event_type: EventType,
    reason: Option<String>,
    message: Option<String>,
    reporting_component: String,
    reporting_instance: String,
    action: String,
    timestamp: u64,
) -> Event {
    let event_name = object_ref.name.to_owned().unwrap_or("unspecified".into());

    Event {
        metadata: kube::core::ObjectMeta {
            generate_name: Some(event_name),
            namespace: object_ref.namespace.to_owned(),
            ..Default::default()
        },
        involved_object: object_ref,
        reason,
        message,
        type_: Some(event_type.to_string()),
        source: None,
        event_time: Some(timestamp.to_time()),
        reporting_component: Some(reporting_component),
        reporting_instance: Some(reporting_instance),
        action: Some(action),
        ..Default::default()
    }
}

#[derive(Clone, Debug)]
pub enum EventType {
    Normal,
    Warning,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::Normal => write!(f, "Normal"),
            EventType::Warning => write!(f, "Warning"),
        }
    }
}

pub trait IntoMicroTimeExt {
    fn to_time(self) -> MicroTime;
}

impl IntoMicroTimeExt for u64 {
    fn to_time(self) -> MicroTime {
        MicroTime(jiff::Timestamp::from_millisecond(self as i64).unwrap())
    }
}
