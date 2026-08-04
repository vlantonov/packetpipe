#pragma once
#include "flow_state.hpp"
#include <cstdint>

namespace packetpipe {

enum class FlowEventType : uint8_t { Start, End, Stats };
enum class ExpireReason   : uint8_t { IdleTimeout, TcpTeardown };

/// A flow lifecycle event passed from the capture pipeline to the Kafka sink.
struct FlowEvent {
    FlowEventType type{FlowEventType::Start};
    FlowState     state{};
    int64_t       event_timestamp_us{0};
    ExpireReason  reason{ExpireReason::IdleTimeout}; ///< valid only for type == End
};

} // namespace packetpipe
