# Release Notes - PacketPipe v0.1.1

Release date: 2026-08-04
Previous version: v0.1.0
Release type: PATCH

## Summary

This patch release stabilizes CI and runtime behavior across dependency/toolchain variants. It contains no schema or API breaking changes and preserves packet, flow, Kafka, and metrics behavior introduced in v0.1.0.

## What Changed

- CI and dependency provisioning:
  - fixed run-vcpkg path and baseline resolution behavior
  - aligned CI vcpkg pinning and toolchain usage
- CMake and build compatibility:
  - hardened avro-cpp target resolution for differing exported target names
  - corrected packet capture and flow table build issues in Linux CI
  - added Linux fallback to link libcap by name when direct discovery fails
- Link/runtime fixes:
  - removed forced httplib OpenSSL path in Schema Registry client to prevent unresolved SSL symbols
- Avro serialization stability:
  - replaced fragile field/union access patterns in Avro serializer
  - switched union handling to GenericDatum branch APIs compatible with avro-cpp 1.12.1
- Tests:
  - fixed includes/warnings in test targets
  - Avro roundtrip regressions resolved

## Validation

- Local validation completed with CI-equivalent toolchain setup:
  - cmake --build --preset release
  - ctest --preset release --output-on-failure
- Result: 55/55 tests passed

## Included Commit Range

From v0.1.0 to v0.1.1:
- 2e0f060 semver(patch): fix ci vcpkg setup path and checkout failure
- 28bcc85 semver(patch): fix run-vcpkg baseline resolution in ci
- 300f45f semver(patch): align ci vcpkg pinning and toolchain path
- 71aaa1f semver(patch): bypass broken avro-cpp cmake config in ci
- a5d9b37 semver(patch): make avro target resolution robust in cmake
- 6c8250a semver(patch): fix pcap and avro header compatibility in ci
- 66e7e31 semver(patch): fix ci compile breaks in flowtable and tests
- fe25c52 semver(patch): fix test includes and clean bpf warnings
- f6d1007 semver(patch): stop forcing httplib openssl path in avro client
- 044c966 semver(patch): stabilize avro serializer field access
- e37acfc semver(patch): fix avro union handling segfault

## Compatibility

- Schema compatibility: unchanged
- Wire format compatibility: unchanged
- Runtime/config compatibility: unchanged

## Next Focus

- Add focused regression tests for Avro field/union behavior across avro-cpp updates
- Keep CI dependency pinning policy explicit to avoid baseline drift
