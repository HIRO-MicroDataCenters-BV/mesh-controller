use chrono::{TimeZone, Utc};
use k8s_openapi::api::core::v1::Event;
use k8s_openapi::api::core::v1::{EventSource, ObjectReference};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;

pub fn create_event(
    object_ref: ObjectReference,
    event_type: EventType,
    reason: Option<String>,
    message: Option<String>,
    source_component: Option<String>,
    timestamp: u64,
) -> Event {
    let source = source_component.map(|component| EventSource {
        component: Some(component),
        ..Default::default()
    });
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
        source,
        event_time: Some(timestamp.to_time()),
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
        MicroTime(Utc.timestamp_millis_opt(self as i64).unwrap())
    }
}
