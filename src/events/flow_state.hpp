#pragma once
#include "flow_key.hpp"
#include <cstdint>

namespace packetpipe {

/// Per-flow counters maintained in the flow table.
struct FlowState {
    FlowKey  key{};
    int64_t  start_us{0};       ///< timestamp of first packet (µs since epoch)
    int64_t  last_seen_us{0};   ///< timestamp of most recent packet
    uint64_t packet_count{0};
    uint64_t byte_count{0};
};

} // namespace packetpipe
