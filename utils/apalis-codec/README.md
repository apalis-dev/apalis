# apalis-codec

A codec utility crate for apalis backends

## Overview

`apalis-codec` provides serialization and deserialization utilities for encoding and decoding job payloads in apalis.
It handles the conversion of task data to and from various formats for reliable storage and transmission.

## Features

- Multiple codec support (JSON, MessagePack, etc.)
- Type-safe serialization
- Error handling for codec operations
- Integration with all apalis backends with codec support via `BackendExt`

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
apalis-codec = { version = "0.1", features = ["msgpack"] }
```

## License

Licensed under the same terms as the apalis project.
