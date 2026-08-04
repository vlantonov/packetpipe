# Release Notes — PacketPipe v0.1.0

**Release date:** 2026-08-04  
**Approved commit:** fe0017b  
**Approved by:** QA Engineer

---

## Summary

First production-ready release of PacketPipe, a C++20 flow-event pipeline that captures network traffic (pcap file or live interface), classifies flows, serializes events to Avro, publishes to Kafka, and exposes Prometheus metrics.

---

## Shipped Capabilities

| Area | Capability |
|---|---|
| Packet ingestion | pcap file replay (`PcapFileSource`) and live interface capture (`LiveCaptureSource`) with BPF filter support |
| Flow tracking | 5-tuple flow table with per-flow state machine (`FlowTable`) |
| Serialization | Avro serialization against a Schema Registry (`AvroSerializer`, `SchemaRegistryClient`) |
| Kafka sink | `AvroKafkaSink` with retry buffering (capped at `sr_buffer_size_`); `KafkaProducer::produce()` returns `bool` to signal local failure |
| Metrics | Prometheus counters via `MetricsRegistry`; `packetpipe_packets_dropped_total` incremented on decode failure in both source types |
| Configuration | CLI-driven `AppConfig` / `ConfigParser` |
| Demo stack | `docker-compose.yml` with Kafka, Schema Registry, Prometheus, and Grafana; dashboard provisioned at `demo/grafana/dashboards/` |
| CI | GitHub Actions pipeline: configure → build → GoogleTest and coverage gate (>= 70% line coverage on testable core) |

---

## Test Coverage

Unit tests pass for: flow key hashing, flow table lifecycle, packet decoder, BPF filter, config parser, Avro round-trip, and Kafka message key encoding.  
Coverage gate: 70 % line coverage enforced via gcovr; six infra-bound files excluded (require live Kafka / Prometheus / libpcap environments).

---

## Known Issues / Post-Release Backlog

| ID | Description | Priority |
|---|---|---|
| BL-001 | **IPv6 decoder test gap** — `PacketDecoder` handles IPv6 in production code but no unit test covers the IPv6 packet path. A test using a crafted IPv6 pcap fixture should be added before any IPv6-specific changes are made. | Medium |
| BL-002 | FR-ING-5 does not cover Linux SLL (any-interface captures via `any` pseudo-interface). | Low |

---

## Upgrade / Deployment Notes

- Requires `libcap-dev` on the build host (documented in README).
- Use `cmake --preset release` / `cmake --build --preset release` to build the release binary.
- Demo stack: `docker compose up -d` from the repo root.

---

## Handoff

This release is documented and ready for ongoing maintenance. The next stage owner is the **Maintenance Engineer**.
