use {
    chrono::{DateTime, Utc},
    serde::Serialize,
    serde_json::Value,
};

#[derive(Debug, Serialize)]
pub struct WssEvent<'a> {
    event: Value,
    created_at: DateTime<Utc>,
    provider: &'a String,
}

impl<'a> WssEvent<'a> {
    pub fn new(message: String, provider: &String) -> WssEvent {
        WssEvent {
            event: json5::from_str(&message).expect("Message could not be deserialized"),
            created_at: Utc::now(),
            provider,
        }
    }

    pub fn to_string(&self) -> String {
        json5::to_string(self).expect("WssEvent count not be serialized")
    }
}
