use crate::database::datatype::SensorId;
use crate::database::tables::SensorEntry;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorsDocument {
    pub sensors: HashMap<SensorId, SensorElement>,
}

impl SensorsDocument {
    pub fn from(values: Vec<SensorEntry>) -> Self {
        Self {
            sensors: values
                .into_iter()
                .map(|entry| (entry.id, entry.into()))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorElement {
    pub name: String,
}

impl SensorElement {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl From<SensorEntry> for SensorElement {
    fn from(val: SensorEntry) -> Self {
        SensorElement { name: val.name }
    }
}
