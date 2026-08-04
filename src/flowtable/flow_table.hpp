#pragma once
#include "config/app_config.hpp"
#include "events/flow_event.hpp"
#include "metrics/imetrics_registry.hpp"
#include "packets/parsed_packet.hpp"
#include <functional>
#include <mutex>
#include <unordered_map>

namespace packetpipe {

/// Maintains the in-memory flow table. Thread-safe between the capture thread
/// (process) and the timer thread (timer_sweep / drain_all) via an internal mutex.
class FlowTable {
public:
    using EventCallback = std::function<bool(FlowEvent)>;

    /// @param cfg      Application configuration.
    /// @param metrics  Metrics handles; must outlive this object.
    /// @param on_event Called for every emitted FlowEvent; returns false if the sink is full.
    FlowTable(const AppConfig& cfg, IMetricsRegistry& metrics, EventCallback on_event);

    /// Called from the capture thread on every decoded packet.
    void process(const ParsedPacket& pkt);

    /// Called periodically from the timer thread.
    /// Emits FlowStats for all active flows and expires idle ones.
    void timer_sweep(int64_t now_us);

    /// Called at shutdown (capture thread, after capture stops).
    /// Emits FlowEnd for every remaining active flow.
    void drain_all(int64_t now_us);

private:
    void emit(FlowEvent evt);
    void expire_flow(std::unordered_map<FlowKey, FlowState, FlowKeyHash>::iterator it,
                     int64_t now_us, ExpireReason reason);

    AppConfig           cfg_;       // by value – cheap to copy
    IMetricsRegistry&   metrics_;
    EventCallback       on_event_;

    std::mutex mutex_;
    std::unordered_map<FlowKey, FlowState, FlowKeyHash> flows_;
    int64_t last_heartbeat_us_{0};
    int64_t idle_timeout_us_{0};
    int64_t stats_interval_us_{0};
    int     drop_count_{0};
};

} // namespace packetpipe
