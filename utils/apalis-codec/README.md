# apalis-codec

`apalis-codec` provides encoding and decoding strategies for task arguments, results, and other values used by backends.
This crate exposes a small, backend-independent [`Codec`] trait and optional implementations for common serialization formats.

## Features

The following codecs are available behind feature flags:

- `json` — JSON encoding using `serde_json`
- `msgpack` — MessagePack encoding using `rmp-serde`
- `bincode` — Binary encoding using `bincode`

No codec implementation is enabled by default.

## Codec

The [`Codec`] trait defines the interface between a value and its compact representation:

```rust
pub trait Codec<T> {
    /// The error type returned if encoding or decoding fails.
    type Error;

    /// The compact or encoded representation of `T`.
    type Compact;

    /// Encode a value of type `T` into its compact representation.
    fn encode(&self, val: &T) -> Result<Self::Compact, Self::Error>;

    /// Decode a compact representation back into a value of type `T`.
    fn decode(&self, val: &Self::Compact) -> Result<T, Self::Error>;
}
```

The `Compact` type is intentionally left up to the codec. Depending on the implementation, it may be a byte buffer, a string, or another representation suitable for storage or transmission.

## Available Codecs

### JSON

Enable the `json` feature:

```toml
[dependencies]
apalis-codec = { version = "...", features = ["json"] }
```

JSON is useful when human-readable representations or interoperability with other systems are important.

### MessagePack

Enable the `msgpack` feature:

```toml
[dependencies]
apalis-codec = { version = "...", features = ["msgpack"] }
```

MessagePack provides a compact binary representation while retaining broad serialization compatibility.

### Bincode

Enable the `bincode` feature:

```toml
[dependencies]
apalis-codec = { version = "...", features = ["bincode"] }
```

Bincode provides a compact binary representation suitable for applications where storage size and serialization performance are important.


## Choosing a Codec

The appropriate codec depends on how tasks are stored or transported:

| Codec       | Representation | Human-readable | Typical use                           |
| ----------- | -------------- | -------------- | ------------------------------------- |
| JSON        | Text           | Yes            | APIs, debugging, interoperability     |
| MessagePack | Binary         | No             | Compact storage and network transport |
| Bincode     | Binary         | No             | Efficient Rust-native serialization   |

For distributed task queues, **MessagePack** or **Bincode** are generally preferable when compact binary representations are desired. JSON can be useful when tasks need to be inspected or consumed by systems outside the Rust ecosystem.

## Serde

The codec implementations are designed around [`serde`](https://serde.rs/). Values generally need to implement the appropriate `Serialize` and `Deserialize` traits.

For example:

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Email {
    to: String,
    subject: String,
}
```

The same task type can then be encoded using different codecs without changing the task itself.

## Feature Flags

```toml
[features]
json = ["serde_json"]
msgpack = ["rmp-serde"]
bincode = ["bincode"]
```

Select only the formats required by your application to avoid unnecessary dependencies.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT License

at your option.
