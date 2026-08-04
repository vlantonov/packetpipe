# PacketPipe

PacketPipe is a C++20 flow telemetry producer:

1. Reads packets from a pcap file or a live interface
2. Builds per-flow lifecycle events (start, end, periodic stats)
3. Serializes events with Apache Avro (open schema in [schemas/FlowEvent.avsc](schemas/FlowEvent.avsc))
4. Publishes to Kafka via librdkafka
5. Exposes Prometheus metrics on `/metrics`

## Requirements

- CMake 3.25+
- C++20 compiler (GCC 13+ or Clang 17+)
- Ninja
- Docker + Docker Compose
- vcpkg (recommended for dependencies)

## Build

```bash
cmake --preset release -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build --preset release
```

## Run Against Pcap

```bash
./build/release/packetpipe \
  --pcap demo/sample.pcap \
  --kafka-brokers localhost:9092 \
  --kafka-topic packetpipe.flows \
  --schema-registry-url http://localhost:8081 \
  --metrics-port 9090
```

## Run Against Live Interface

```bash
./build/release/packetpipe \
  --iface eth0 \
  --filter "tcp or udp" \
  --kafka-brokers localhost:9092 \
  --schema-registry-url http://localhost:8081
```

Live capture requires CAP_NET_RAW on Linux:

```bash
sudo setcap cap_net_raw+eip ./build/release/packetpipe
```

## One-Command Demo Stack

Start Kafka, Schema Registry, Prometheus, and Grafana:

```bash
docker compose up -d
```

Access:

- Grafana: http://localhost:3000 (`admin` / `admin`)
- Prometheus: http://localhost:9091
- Schema Registry: http://localhost:8081
- Kafka bootstrap: localhost:9092

Then start PacketPipe in another shell with the pcap command above.

## Tests

```bash
ctest --preset release
```

## Metrics

PacketPipe exports these metrics:

- `packetpipe_packets_received_total`
- `packetpipe_packets_dropped_total`
- `packetpipe_flows_active`
- `packetpipe_flows_created_total`
- `packetpipe_flows_expired_total{reason=...}`
- `packetpipe_kafka_messages_produced_total`
- `packetpipe_kafka_delivery_errors_total`
- `packetpipe_avro_serialization_errors_total`
- `packetpipe_schema_registry_retries_total`
