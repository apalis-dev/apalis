use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt::Debug};

use apalis_core::task::task_id::{RandomId, TaskId};

use crate::JsonMapMetadata;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTask {
    pub(super) task_id: Option<TaskId<RandomId>>,
    pub(super) args: serde_json::Value,
    pub(super) ctx: JsonMapMetadata,
    pub(super) result: Option<serde_json::Value>,
    pub(super) idempotency_key: Option<String>,
}

/// Flattens a `serde_json::Value` into a `BTreeMap<String, String>`.
///
/// Arrays and objects are flattened using dot notation.
///
/// `prefix` is prepended to every generated key.
///
/// Example:
///
/// prefix = Some("root")
///
/// {
///   "user": {
///     "name": "John"
///   }
/// }
///
/// becomes:
///
/// root.user.name => "John"
///
pub(crate) fn from_value(prefix: Option<&str>, value: &Value) -> BTreeMap<String, String> {
    fn recurse(prefix: Option<String>, value: &Value, output: &mut BTreeMap<String, String>) {
        match value {
            Value::Object(map) => {
                for (key, value) in map {
                    let path = match &prefix {
                        Some(prefix) => format!("{prefix}.{key}"),
                        None => key.clone(),
                    };

                    recurse(Some(path), value, output);
                }
            }

            Value::Array(values) => {
                for (index, value) in values.iter().enumerate() {
                    let path = match &prefix {
                        Some(prefix) => format!("{prefix}.{index}"),
                        None => index.to_string(),
                    };

                    recurse(Some(path), value, output);
                }
            }

            Value::Null => {
                if let Some(prefix) = prefix {
                    output.insert(prefix, "null".to_owned());
                }
            }

            Value::Bool(v) => {
                if let Some(prefix) = prefix {
                    output.insert(prefix, v.to_string());
                }
            }

            Value::Number(v) => {
                if let Some(prefix) = prefix {
                    output.insert(prefix, v.to_string());
                }
            }

            Value::String(v) => {
                if let Some(prefix) = prefix {
                    output.insert(prefix, v.clone());
                }
            }
        }
    }

    let mut output = BTreeMap::new();

    recurse(prefix.map(|s| s.to_owned()), value, &mut output);

    output
}

/// Reconstructs a nested `serde_json::Value` from a flattened
/// `BTreeMap<String, String>`.
///
/// If `prefix` is provided, only matching keys are used and the prefix
/// is stripped before reconstruction.
///
/// Example:
///
/// root.user.name => "John"
///
/// with prefix Some("root")
///
/// becomes:
///
/// {
///   "user": {
///     "name": "John"
///   }
/// }
///
pub(crate) fn to_value(prefix: Option<&str>, map: &BTreeMap<String, String>) -> Value {
    fn parse_scalar(value: &str) -> Value {
        if value == "null" {
            Value::Null
        } else if value == "true" {
            Value::Bool(true)
        } else if value == "false" {
            Value::Bool(false)
        } else if let Ok(n) = value.parse::<i64>() {
            Value::Number(n.into())
        } else if let Ok(n) = value.parse::<f64>() {
            serde_json::Number::from_f64(n)
                .map(Value::Number)
                .unwrap_or_else(|| Value::String(value.to_owned()))
        } else {
            Value::String(value.to_owned())
        }
    }

    fn insert_path(root: &mut Value, parts: &[&str], value: Value) {
        if parts.is_empty() {
            *root = value;
            return;
        }

        let current = parts[0];
        let is_index = current.parse::<usize>().is_ok();

        if parts.len() == 1 {
            match (is_index, root) {
                (true, Value::Array(arr)) => {
                    let idx = current.parse::<usize>().unwrap();

                    if arr.len() <= idx {
                        arr.resize(idx + 1, Value::Null);
                    }

                    arr[idx] = value;
                }

                (false, Value::Object(map)) => {
                    map.insert(current.to_owned(), value);
                }

                (true, slot) => {
                    let idx = current.parse::<usize>().unwrap();

                    let mut arr = Vec::new();
                    arr.resize(idx + 1, Value::Null);
                    arr[idx] = value;

                    *slot = Value::Array(arr);
                }

                (false, slot) => {
                    let mut map = serde_json::Map::new();
                    map.insert(current.to_owned(), value);
                    *slot = Value::Object(map);
                }
            }

            return;
        }

        match (is_index, root) {
            (true, Value::Array(arr)) => {
                let idx = current.parse::<usize>().unwrap();

                if arr.len() <= idx {
                    arr.resize(idx + 1, Value::Null);
                }

                insert_path(&mut arr[idx], &parts[1..], value);
            }

            (false, Value::Object(map)) => {
                let next = map.entry(current.to_owned()).or_insert(Value::Null);

                insert_path(next, &parts[1..], value);
            }

            (true, slot) => {
                let idx = current.parse::<usize>().unwrap();

                let mut arr = Vec::new();
                arr.resize(idx + 1, Value::Null);

                *slot = Value::Array(arr);

                if let Value::Array(arr) = slot {
                    insert_path(&mut arr[idx], &parts[1..], value);
                }
            }

            (false, slot) => {
                *slot = Value::Object(serde_json::Map::new());

                if let Value::Object(map) = slot {
                    let next = map.entry(current.to_owned()).or_insert(Value::Null);

                    insert_path(next, &parts[1..], value);
                }
            }
        }
    }

    let mut root = Value::Object(serde_json::Map::new());

    for (key, value) in map {
        let stripped = match prefix {
            Some(prefix) => {
                if key == prefix {
                    ""
                } else if let Some(rest) = key.strip_prefix(&format!("{prefix}.")) {
                    rest
                } else {
                    continue;
                }
            }

            None => key.as_str(),
        };

        if stripped.is_empty() {
            root = parse_scalar(value);
            continue;
        }

        let parts: Vec<&str> = stripped.split('.').collect();

        insert_path(&mut root, &parts, parse_scalar(value));
    }

    root
}
