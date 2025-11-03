[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/HIRO-MicroDataCenters-BV/mesh-controller)

# Mesh Controller for Decentralized Control Plane

## Usage

1. Copy the [configuration file](config.example.yaml) and adjust it to your setup: `cp config.example.yaml config.yaml`
2. Generate hexadecimal-encoded Ed25519 private key file:
    ```bash
    # Generate random Ed25519 private key via openssl
    openssl genpkey -algorithm ed25519 -outform der -out private-key.hex

    # Convert it to hexadecimal representation via xxd
    echo -n $(xxd -plain -cols 32 -s -32 private-key.hex) > private-key.hex
    ```
3. Run the `mesh-controller` process via `mesh-controller -c config.yaml -k private-key.hex`

## Development

### Prerequisites

* Rust 1.86.0+

### Installation and running

1. Launch mesh controller
```bash
# Run with default configurations
cargo run

# Pass additional arguments
cargo run -- --config config.yaml
```
2. Configure log level
```bash
# Enable additional logging
cargo run -- --log-level "DEBUG"

# Enable logging for specific target
cargo run -- --log-level "mesh=DEBUG"

# Enable logging for _all_ targets
cargo run -- --log-level "=TRACE"
```
3. Run tests, linters and format checkers
```bash
cargo test
cargo clippy
cargo fmt
```
4. Build for production
```bash
cargo build --release
```

## License

[MIT](LICENSE)
