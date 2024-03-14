use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::resp::RedisValue;

pub struct DataValue {
    value: RedisValue,
    expiry: Option<Duration>,
    inserted_at: Instant,
}

impl DataValue {
    pub fn value(&self) -> &RedisValue {
        &self.value
    }
}

pub struct Storage {
    data: HashMap<String, DataValue>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn get(&self, key: String) -> Option<&DataValue> {
        match self.data.get(&key) {
            Some(d) => {
                if let Some(expiry) = d.expiry {
                    if d.inserted_at.elapsed().as_millis() > expiry.as_millis() {
                        return None;
                    }
                }
                Some(d)
            }
            None => None,
        }
    }

    pub fn set(&mut self, key: String, value: RedisValue, expiry: Option<u64>) {
        self.data.insert(
            key,
            DataValue {
                value,
                expiry: match expiry {
                    Some(e) => Some(Duration::from_millis(e)),
                    None => None,
                },
                inserted_at: Instant::now(),
            },
        );
    }
}
