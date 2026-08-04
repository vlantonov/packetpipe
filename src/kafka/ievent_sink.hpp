#pragma once
#include "events/flow_event.hpp"
#include <chrono>

namespace packetpipe {

/// Abstract event sink – decouples the flow table from the Kafka pipeline.
class IEventSink {
public:
    virtual ~IEventSink() = default;

    /// Non-blocking. Returns false if the internal queue is full.
    virtual bool push(FlowEvent evt) noexcept = 0;

    /// Block until all pushed events have been produced to Kafka or timeout elapses.
    virtual void flush(std::chrono::milliseconds timeout) = 0;
};

} // namespace packetpipe
