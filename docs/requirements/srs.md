# PacketPipe — Software Requirements Specification

**Version:** 1.0  
**Date:** 2026-08-04  
**Author:** Requirements Analyst  
**License:** MIT  
**Status:** Draft — awaiting System Architect review

---

## 1. Overview

PacketPipe is a C++20 command-line daemon that captures network packets from either offline pcap files or a live network interface, reconstructs logical flows, emits per-flow lifecycle events, serializes those events with Apache Avro (schema defined in open `.avsc` IDL files), and publishes them to an Apache Kafka topic via librdkafka. A built-in HTTP endpoint exposes Prometheus-compatible `/metrics`. A single `docker-compose up` command starts the full observability stack (Kafka, Prometheus, Grafana) with a pre-provisioned Grafana dashboard for portfolio demonstration.

---

## 2. Functional Requirements

### 2.1 Packet Ingestion

| ID | Requirement |
|----|-------------|
| FR-ING-1 | The system shall accept a pcap file path via a CLI flag (`--pcap <file>`) and process all packets from that file. |
| FR-ING-2 | The system shall accept a live network interface name via a CLI flag (`--iface <name>`) and capture packets in real time using libpcap. |
| FR-ING-3 | Exactly one of `--pcap` or `--iface` shall be required per invocation; supplying neither or both shall exit with a non-zero status and a human-readable error message. |
| FR-ING-4 | The system shall support an optional BPF filter string via `--filter <expression>` applied before any flow processing. |
| FR-ING-5 | The system shall process at minimum Ethernet frames carrying IPv4 and IPv6 packets. |
| FR-ING-6 | The system shall parse TCP, UDP, and ICMP/ICMPv6 transport-layer protocols. Packets with unrecognised upper-layer protocols shall be counted and discarded without crashing. |

### 2.2 Flow Reconstruction

| ID | Requirement |
|----|-------------|
| FR-FLOW-1 | The system shall group packets into flows identified by the 5-tuple: {source IP, destination IP, source port, destination port, IP protocol}. For ICMP the tuple shall use ICMP type and code in place of ports. |
| FR-FLOW-2 | A flow shall be considered **new** on receipt of its first packet. |
| FR-FLOW-3 | A flow shall be considered **expired** when no packet matching its 5-tuple has been seen within a configurable idle-timeout interval (`--flow-timeout <seconds>`, default 60 s). |
| FR-FLOW-4 | For TCP flows, receipt of a FIN+ACK or RST exchange shall immediately mark the flow as expired regardless of the idle timeout. |
| FR-FLOW-5 | The system shall track, per flow: packet count, byte count, start timestamp (first packet), last-seen timestamp, and flow direction (initiator side is the source of the first packet). |
| FR-FLOW-6 | A hard limit on simultaneously tracked flows shall be configurable (`--max-flows <N>`, default 100 000). When the limit is reached the system shall emit a warning metric and drop new flows (not crash). |

### 2.3 Event Emission

| ID | Requirement |
|----|-------------|
| FR-EVT-1 | The system shall emit a **FlowStart** event when a new flow is first created. |
| FR-EVT-2 | The system shall emit a **FlowEnd** event when a flow expires (idle timeout or TCP teardown). |
| FR-EVT-3 | The system shall emit a **FlowStats** heartbeat event at a configurable interval (`--stats-interval <seconds>`, default 30 s) for each active flow, containing cumulative counters at the time of emission. |
| FR-EVT-4 | Each event shall carry the fields defined in the canonical Avro schema (see FR-SER-1). |
| FR-EVT-5 | Event timestamps shall be expressed as microseconds since Unix epoch (int64). |

### 2.4 Avro Serialization

| ID | Requirement |
|----|-------------|
| FR-SER-1 | All event types (FlowStart, FlowEnd, FlowStats) shall be described in human-editable Apache Avro IDL files (`.avsc` JSON schema format) committed to the repository under `schemas/`. |
| FR-SER-2 | The Avro schema shall be versioned (a `schema_version` string field). Schema version shall follow SemVer major.minor notation. |
| FR-SER-3 | The system shall serialize each event using the Avro binary encoding together with the Confluent Schema Registry wire format (magic byte 0x00 + 4-byte schema ID + binary payload). |
| FR-SER-4 | If the Schema Registry is unavailable at startup the system shall fail fast with a clear error. If it becomes unavailable during operation the system shall buffer up to a configurable number of events (`--sr-buffer <N>`, default 1 000) and retry before dropping. |
| FR-SER-5 | The schema ID shall be resolved from the Confluent Schema Registry URL, configurable via `--schema-registry-url <url>` (default `http://localhost:8081`). |

### 2.5 Kafka Publishing

| ID | Requirement |
|----|-------------|
| FR-KAF-1 | The system shall publish serialized events to a Kafka topic configurable via `--kafka-topic <name>` (default `packetpipe.flows`). |
| FR-KAF-2 | The Kafka broker list shall be configurable via `--kafka-brokers <host:port,...>` (default `localhost:9092`). |
| FR-KAF-3 | The Kafka message key shall be the flow's 5-tuple encoded as a canonical string so that all events for a single flow land on the same partition. |
| FR-KAF-4 | The system shall use librdkafka's asynchronous producer with delivery reports; failed deliveries shall increment a dedicated Prometheus counter. |
| FR-KAF-5 | At graceful shutdown (SIGINT / SIGTERM) the system shall flush the Kafka producer queue and wait up to 10 s for outstanding messages to drain before exiting. |
| FR-KAF-6 | Kafka producer configuration (compression codec, batch size, linger ms) shall be settable via an optional librdkafka configuration file whose path is passed with `--kafka-conf <file>`. |

### 2.6 Metrics Endpoint

| ID | Requirement |
|----|-------------|
| FR-MET-1 | The system shall expose a Prometheus text-format scrape endpoint at `http://0.0.0.0:<port>/metrics`, where `<port>` is configurable via `--metrics-port <N>` (default 9090). |
| FR-MET-2 | The following metrics shall be present: |
| FR-MET-2a | `packetpipe_packets_received_total` — counter, packets ingested. |
| FR-MET-2b | `packetpipe_packets_dropped_total` — counter, packets discarded (unrecognised protocol or over flow limit). |
| FR-MET-2c | `packetpipe_flows_active` — gauge, currently tracked flows. |
| FR-MET-2d | `packetpipe_flows_created_total` — counter, total flows opened. |
| FR-MET-2e | `packetpipe_flows_expired_total` — counter, total flows expired (labelled by reason: `idle_timeout`, `tcp_teardown`). |
| FR-MET-2f | `packetpipe_kafka_messages_produced_total` — counter, messages handed to librdkafka. |
| FR-MET-2g | `packetpipe_kafka_delivery_errors_total` — counter, failed Kafka deliveries. |
| FR-MET-2h | `packetpipe_avro_serialization_errors_total` — counter, Avro serialization failures. |
| FR-MET-2i | `packetpipe_schema_registry_retries_total` — counter, Schema Registry retry attempts. |
| FR-MET-3 | All metrics shall include a `version` label whose value matches the application's SemVer string. |

### 2.7 Demo Environment

| ID | Requirement |
|----|-------------|
| FR-DEMO-1 | A `docker-compose.yml` at the repository root shall start, with a single `docker-compose up` command, all services required to run the full pipeline: Kafka (with Zookeeper or KRaft), Confluent Schema Registry, Prometheus, and Grafana. |
| FR-DEMO-2 | The Grafana service shall be pre-configured via provisioning files (committed under `demo/grafana/provisioning/`) to add the Prometheus data source automatically without manual UI steps. |
| FR-DEMO-3 | A Grafana dashboard JSON file (committed under `demo/grafana/dashboards/`) shall be provisioned automatically and display at minimum: packets received rate, active flows gauge, Kafka message rate, and Kafka delivery error rate. |
| FR-DEMO-4 | The `docker-compose.yml` shall expose Grafana on `localhost:3000`, Prometheus on `localhost:9091`, and Kafka on `localhost:9092` with no port conflicts among the defined services. |
| FR-DEMO-5 | A `README.md` quick-start section shall document the exact sequence of commands needed to run the demo from a clean clone (including any necessary `docker network` or `docker volume` steps). |
| FR-DEMO-6 | A sample pcap file (≤ 5 MB) shall be committed under `demo/` to allow offline demonstration without a live interface or root privileges. |

---

## 3. Non-Functional Requirements

### 3.1 Performance

| ID | Requirement |
|----|-------------|
| NFR-PERF-1 | When replaying a pcap file, the system shall process packets at a rate not less than 100 000 packets per second on a single modern x86-64 core (measured with the sample demo pcap). |
| NFR-PERF-2 | Steady-state resident memory usage with 100 000 active flows shall not exceed 512 MB. |
| NFR-PERF-3 | End-to-end latency from packet capture to Kafka produce call (excluding network RTT) shall be ≤ 5 ms at the 99th percentile under the reference workload. |

### 3.2 Reliability

| ID | Requirement |
|----|-------------|
| NFR-REL-1 | The system shall not crash or exhibit undefined behaviour on any valid pcap file produced by Wireshark/tcpdump. |
| NFR-REL-2 | The system shall handle malformed or truncated packets (reported by libpcap) without crashing; such packets shall be counted by `packetpipe_packets_dropped_total`. |
| NFR-REL-3 | SIGINT and SIGTERM shall trigger a clean shutdown: all active flow states shall be finalized (FlowEnd events emitted) and the Kafka queue drained. |

### 3.3 Observability

| ID | Requirement |
|----|-------------|
| NFR-OBS-1 | Structured logs shall be emitted to stdout in a format compatible with common log aggregators (plain-text with ISO-8601 timestamps and log level prefix at minimum). |
| NFR-OBS-2 | Log verbosity shall be controllable via `--log-level <level>` accepting `error`, `warn`, `info`, `debug`. |

### 3.4 Portability and Standards

| ID | Requirement |
|----|-------------|
| NFR-PORT-1 | The codebase shall conform to the C++20 standard and compile without warnings under GCC ≥ 13 and Clang ≥ 17 with `-Wall -Wextra -Wpedantic`. |
| NFR-PORT-2 | The primary supported platform is Linux (kernel ≥ 5.10, x86-64). Windows and macOS support are out of scope for this iteration. |
| NFR-PORT-3 | Live capture (`--iface`) on Linux shall require CAP_NET_RAW or equivalent privilege; the system shall check for this at startup and exit with an actionable error if unprivileged. |

### 3.5 Build and Test

| ID | Requirement |
|----|-------------|
| NFR-BUILD-1 | The project shall use CMake ≥ 3.25 as the sole build system. A top-level `CMakePresets.json` shall define at minimum a `debug` and a `release` configure preset. |
| NFR-BUILD-2 | All external library dependencies shall be managed through a declarative mechanism committed to the repository (vcpkg `vcpkg.json` manifest or CMake `FetchContent` blocks) so that a fresh clone builds with a single `cmake --preset release && cmake --build` invocation. |
| NFR-BUILD-3 | The test suite shall use GoogleTest and be runnable via `ctest --preset release`. |
| NFR-BUILD-4 | Unit tests shall cover: flow 5-tuple keying, flow idle-timeout expiry logic, BPF filter application, Avro schema round-trip (serialize → deserialize), and Kafka message key encoding. |
| NFR-BUILD-5 | A CI workflow file (GitHub Actions) shall build and run the full test suite on every push to `main` and every pull request. |
| NFR-BUILD-6 | Code coverage (line coverage) for the unit-testable components shall be ≥ 70 %, measured by the CI pipeline. |

---

## 4. External Dependencies and Assumptions

| Item | Detail |
|------|--------|
| libpcap | System-installed libpcap (≥ 1.10) or vcpkg-provided equivalent; POSIX raw-socket capture API. |
| librdkafka | Apache Kafka C/C++ client library; version ≥ 2.3. |
| Apache Avro C++ | Official Apache Avro C++ library for schema parsing and binary encoding. |
| Confluent Schema Registry | Accessed over HTTP; the demo environment shall provide a containerised instance. |
| prometheus-cpp | Pull-mode metrics; linked statically or dynamically; version ≥ 1.2. |
| Docker / docker-compose | Required for the demo environment only; not a runtime dependency of the binary. |
| C++ standard library | No Boost is assumed; prefer standard-library solutions for threading, containers, and time. |
| Assumption A-1 | The build machine has network access to download vcpkg packages or FetchContent sources at configure time. |
| Assumption A-2 | The target Linux system provides kernel-level BPF support (`/proc/net/dev` accessible for interface enumeration). |
| Assumption A-3 | Schema Registry is reachable before the first event is produced; the system does not need to auto-register schemas — schemas are pre-registered as part of the demo setup. |
| Assumption A-4 | The demo pcap file contains real multi-flow TCP and UDP traffic to exercise all event types. |

---

## 5. Constraints and Scope Boundaries

| ID | Constraint / Out-of-Scope Item |
|----|-------------------------------|
| SC-1 | **No packet reassembly or deep packet inspection (DPI)** beyond L3/L4 headers is required in this iteration. Application-layer payload decoding is out of scope. |
| SC-2 | **No Kafka consumer** is required; PacketPipe is a producer-only application. |
| SC-3 | **No TLS** for Kafka or Schema Registry connections in this iteration. Security hardening is deferred. |
| SC-4 | **No persistent flow state** across restarts; all in-memory flow tables are ephemeral. |
| SC-5 | **No GUI** or web-based management interface; the Prometheus/Grafana stack provides all visualisation. |
| SC-6 | **No Windows or macOS** support in this iteration (see NFR-PORT-2). |
| SC-7 | **No Kafka consumer group offset tracking or exactly-once semantics**; at-least-once delivery is acceptable. |
| SC-8 | The Grafana dashboard shall be a pre-built static JSON artefact; dynamic dashboard generation is out of scope. |

---

## 6. Open Questions

| ID | Question | Owner |
|----|----------|-------|
| OQ-1 | Should FlowStats heartbeat events be emitted for flows that have had zero new packets since the last heartbeat, or only for flows with activity in the interval? | Product Owner |
| OQ-2 | Is bi-directional flow merging required (i.e., are A→B and B→A the same flow), or should each direction be tracked as a separate 5-tuple? | Product Owner |
| OQ-3 | What Avro schema compatibility mode should be enforced in the Schema Registry (BACKWARD, FULL, NONE)? | Product Owner |
| OQ-4 | Should the demo `docker-compose.yml` also build the `packetpipe` binary inside a container, or does it assume the binary is pre-built on the host? | Product Owner |
| OQ-5 | Is Kafka topic auto-creation acceptable, or must the topic be pre-created with a specific partition count/replication factor in the demo setup? | Product Owner |
| OQ-6 | What is the intended Kafka message retention policy for the demo (default 7 days is likely acceptable but should be confirmed)? | Product Owner |
| OQ-7 | Should the `/metrics` endpoint be served by a background thread within the main process, or as a child process/sidecar? (Architectural question — deferred to System Architect, listed here for awareness.) | System Architect |

---

## 7. Acceptance Criteria

The following checks, executable in a CI or demo environment, constitute done:

| ID | Acceptance Check |
|----|-----------------|
| AC-1 | `./packetpipe --pcap demo/sample.pcap --kafka-brokers localhost:9092 --schema-registry-url http://localhost:8081` exits with code 0 and logs at least one FlowStart and one FlowEnd event. |
| AC-2 | `kafka-console-consumer --topic packetpipe.flows --from-beginning` (after demo pcap run) receives ≥ 1 Avro-encoded message decodable by `avro-tools` against the committed `.avsc` schema. |
| AC-3 | `curl -s http://localhost:9090/metrics` returns HTTP 200 and the response body contains all nine metric names listed in FR-MET-2a through FR-MET-2i. |
| AC-4 | After `docker-compose up`, navigating to `http://localhost:3000` (Grafana) shows the pre-provisioned dashboard without any manual configuration. |
| AC-5 | `ctest --preset release` reports 0 failures; all unit tests in NFR-BUILD-4 have corresponding test cases. |
| AC-6 | Running `./packetpipe` without `--pcap` or `--iface` exits with non-zero code and prints a usage hint (FR-ING-3). |
| AC-7 | Sending SIGTERM to a running `packetpipe` process causes it to emit FlowEnd events for all tracked flows and exit cleanly within 15 s (NFR-REL-3, FR-KAF-5). |
| AC-8 | The project builds from a clean clone with no manual steps beyond `cmake --preset release && cmake --build --preset release` on an Ubuntu 24.04 environment with Docker and CMake ≥ 3.25 installed (NFR-BUILD-2). |
| AC-9 | GCC 13 and Clang 17 both produce zero warnings with `-Wall -Wextra -Wpedantic` on the release preset (NFR-PORT-1). |
| AC-10 | CI pipeline (GitHub Actions) passes on a push to `main` with all tests green and code coverage ≥ 70 % reported (NFR-BUILD-5, NFR-BUILD-6). |

---

## 8. Handoff Statement

Requirements are complete and ready for the **System Architect agent** to design against.

The architect should address at minimum: component decomposition (ingestion, flow-table, event bus, Avro serializer, Kafka producer, metrics server); threading model and synchronisation boundaries; libpcap dispatch-loop integration with the rest of the pipeline; CMake project layout and dependency management strategy; and resolution of open questions OQ-1 through OQ-6 before design is finalised.
