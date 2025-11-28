[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/HIRO-MicroDataCenters-BV/mesh-controller)

# Mesh Controller for Decentralized Control Plane

A Kubernetes controller that enables decentralized peer-to-peer communication and coordination across multiple clusters using the [p2panda](https://github.com/p2panda/p2panda) protocol.

## Overview

The Mesh Controller creates a decentralized control plane for Kubernetes clusters, allowing them to discover and communicate with each other without relying on a central authority. It uses peer-to-peer networking to synchronize state and enables multi-cluster coordination.

### Key Features

- **Decentralized Architecture**: No single point of failure - clusters communicate directly via p2panda protocol
- **Peer Discovery**: Automatic discovery of mesh peers across zones and clusters
- **Kubernetes Native**: Custom Resource Definitions (CRDs) for managing mesh peers
- **Security**: Ed25519 cryptographic keys for peer authentication
- **Observability**: Built-in Prometheus metrics and health endpoints
- **Multi-Zone Support**: Coordinate clusters across different zones or regions

## Architecture

The mesh controller consists of several components:

- **Network Layer**: P2P networking using p2panda for decentralized communication
- **Mesh Resource Manager**: Manages `MeshPeer` custom resources in Kubernetes
- **Discovery Service**: Discovers and maintains connections to other mesh peers
- **Synchronization**: Keeps state synchronized across all connected peers

### MeshPeer Custom Resource

The controller manages `MeshPeer` resources that represent other nodes in the mesh network:

```yaml
apiVersion: dcp.hiro.io/v1
kind: MeshPeer
metadata:
  name: example-peer
spec:
  identity:
    publicKey: "ed25519-public-key-hex"
    endpoints:
      - "peer.example.com:9102"
status:
  status: Ready
  instance:
    zone: "us-west"
    start_time: "2024-01-01T00:00:00Z"
```

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

## Kubernetes Deployment

### Using Helm

The mesh controller can be deployed to Kubernetes using the provided Helm chart:

```bash
# Install the mesh controller
helm install mesh-controller ./charts/mesh-controller

# Install with custom values
helm install mesh-controller ./charts/mesh-controller -f custom-values.yaml

# Upgrade an existing installation
helm upgrade mesh-controller ./charts/mesh-controller
```

### Custom Resource Definitions

The controller automatically installs the following CRDs:

- `meshpeers.dcp.hiro.io` - Represents mesh peer nodes in the network

View mesh peers:
```bash
kubectl get meshpeers
kubectl describe meshpeer <peer-name>
```

## Configuration

The mesh controller supports the following configuration options in `config.yaml`:

| Option | Description | Default |
|--------|-------------|---------|
| `bind_port` | Port for mesh communication | 9102 |
| `http_bind_port` | Port for HTTP health endpoint | 3000 |
| `network_id` | Network identifier for peer isolation | "default" |
| `private_key_path` | Path to Ed25519 private key file | - |
| `nodes` | List of known peer nodes to connect to | [] |
| `kubernetes.incluster` | Enable Kubernetes in-cluster mode | false |
| `mesh.zone` | Zone identifier for this instance | - |
| `log_level` | Logging verbosity (ERROR, WARN, INFO, DEBUG, TRACE) | INFO |

See [config.example.yaml](config.example.yaml) for a complete configuration example.

## Monitoring

The mesh controller exposes metrics and health endpoints:

- **Health Check**: `http://localhost:3000/health` - Returns the health status of the controller
- **Metrics**: Prometheus metrics are exposed for monitoring peer connections, sync status, and resource usage

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

## Project Structure

The project is organized as a Rust workspace with multiple crates:

- **mesh**: Main mesh controller application and library
- **mesh-kube**: Kubernetes integration and client wrapper
- **mesh-kube-fake-api**: Mock Kubernetes API for testing
- **mesh-resource**: MeshPeer CRD and resource management
- **anyapplication**: Generic Kubernetes application resource handling

## Contributing

Contributions are welcome! Please ensure that:

1. All tests pass: `cargo test`
2. Code is properly formatted: `cargo fmt`
3. No linter warnings: `cargo clippy`

## License

[MIT](LICENSE)
