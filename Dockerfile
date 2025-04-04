FROM rust:1-alpine AS builder
WORKDIR /usr/src/mesh-controller
COPY . .
RUN apk add musl-dev libressl libressl-dev
RUN cargo install --path ./mesh-controller

FROM rust:1-alpine
RUN apk add musl-dev libressl libressl-dev
COPY --from=builder /usr/local/cargo/bin/mesh-controller /usr/local/bin/mesh-controller
CMD ["/usr/local/bin/mesh-controller", "-c", "/etc/mesh-controller/config.yaml"]
