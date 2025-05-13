use kube::{
    Resource,
    api::{ListParams, ObjectList},
    runtime::reflector::ObjectRef,
};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;
