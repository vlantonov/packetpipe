# PacketPipe SDLC Status

## Active Workstream
- Scope: New C++20 flow-event pipeline project (pcap/live capture -> Avro -> Kafka -> Prometheus metrics -> compose demo stack)
- Current stage: Implementation complete; pending QA validation

## Stage Progress
- Requirements Analyst: Complete (SRS drafted in `docs/requirements/srs.md`)
- System Architect: Complete (design doc in `docs/design/architecture.md`)
- Cpp Developer: Complete (project scaffold, source, tests, demo stack, CI)
- QA Engineer: Not started
- Release Engineer: Not started

## Blockers / Open Questions
- OQ-1..OQ-7 resolved in architecture.md section 1
- Local build/test blocked until external dependencies are provisioned (vcpkg/toolchain not present in environment)
- Flagged to Requirements Analyst: FR-ING-5 does not cover Linux SLL (any-interface captures)

## Notes
- Work will proceed one stage at a time with per-stage SemVer commits.
