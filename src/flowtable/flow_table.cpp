#include "flow_table.hpp"
#include "config/app_config.hpp"
#include <netinet/in.h>
#include <spdlog/spdlog.h>
#include <vector>

namespace packetpipe {

namespace {

FlowKey key_from_packet(const ParsedPacket& pkt) noexcept {
    FlowKey k;
    k.src_ip   = pkt.src_ip;
    k.dst_ip   = pkt.dst_ip;
    k.src_port = pkt.src_port;
    k.dst_port = pkt.dst_port;
    k.protocol = pkt.protocol;
    return k;
}

} // namespace

FlowTable::FlowTable(const AppConfig& cfg, IMetricsRegistry& metrics, EventCallback on_event)
    : cfg_(cfg)
    , metrics_(metrics)
    , on_event_(std::move(on_event))
    , idle_timeout_us_(static_cast<int64_t>(cfg.flow_timeout_sec) * 1'000'000LL)
    , stats_interval_us_(static_cast<int64_t>(cfg.stats_interval_sec) * 1'000'000LL) {
    flows_.reserve(static_cast<size_t>(cfg.max_flows));
}

void FlowTable::emit(FlowEvent evt) {
    if (!on_event_(std::move(evt))) {
        metrics_.packets_dropped().Increment();
        spdlog::warn("[flowtable] event queue full – dropping event");
    }
}

void FlowTable::expire_flow(
    std::unordered_map<FlowKey, FlowState, FlowKeyHash>::iterator it,
    int64_t now_us, ExpireReason reason) {
    FlowEvent evt;
    evt.type              = FlowEventType::End;
    evt.state             = it->second;
    evt.event_timestamp_us = now_us;
    evt.reason            = reason;
    emit(std::move(evt));
    flows_.erase(it);
    metrics_.flows_active().Decrement();
    metrics_.flows_expired(reason).Increment();
}

void FlowTable::process(const ParsedPacket& pkt) {
    metrics_.packets_received().Increment();
    const FlowKey key = key_from_packet(pkt);

    std::lock_guard<std::mutex> lk(mutex_);

    auto it = flows_.find(key);
    if (it == flows_.end()) {
        // New flow
        if (static_cast<int>(flows_.size()) >= cfg_.max_flows) {
            metrics_.packets_dropped().Increment();
            if (++drop_count_ % 1000 == 1) {
                spdlog::warn("[flowtable] max_flows limit reached ({}) – dropping new flows",
                             cfg_.max_flows);
            }
            return;
        }

        FlowState state;
        state.key          = key;
        state.start_us     = pkt.timestamp_us;
        state.last_seen_us = pkt.timestamp_us;
        state.packet_count = 1;
        state.byte_count   = pkt.wire_length;
        flows_.emplace(key, state);

        FlowEvent evt;
        evt.type              = FlowEventType::Start;
        evt.state             = state;
        evt.event_timestamp_us = pkt.timestamp_us;
        emit(std::move(evt));
        metrics_.flows_active().Increment();
        metrics_.flows_created().Increment();
    } else {
        // Existing flow – update counters
        it->second.last_seen_us  = pkt.timestamp_us;
        it->second.packet_count += 1;
        it->second.byte_count   += pkt.wire_length;

        // TCP teardown detection (FR-FLOW-4)
        if (pkt.protocol == IPPROTO_TCP && (pkt.tcp_fin || pkt.tcp_rst)) {
            expire_flow(it, pkt.timestamp_us, ExpireReason::TcpTeardown);
        }
    }
}

void FlowTable::timer_sweep(int64_t now_us) {
    std::lock_guard<std::mutex> lk(mutex_);

    const bool emit_stats = (now_us - last_heartbeat_us_) >= stats_interval_us_;
    std::vector<FlowKey> to_expire;

    for (auto& [key, state] : flows_) {
        if ((now_us - state.last_seen_us) >= idle_timeout_us_) {
            to_expire.push_back(key);
        } else if (emit_stats) {
            FlowEvent evt;
            evt.type              = FlowEventType::Stats;
            evt.state             = state;
            evt.event_timestamp_us = now_us;
            emit(std::move(evt));
        }
    }

    for (const auto& key : to_expire) {
        auto it = flows_.find(key);
        if (it != flows_.end()) expire_flow(it, now_us, ExpireReason::IdleTimeout);
    }

    if (emit_stats) last_heartbeat_us_ = now_us;
}

void FlowTable::drain_all(int64_t now_us) {
    std::lock_guard<std::mutex> lk(mutex_);
    for (auto it = flows_.begin(); it != flows_.end(); ) {
        FlowEvent evt;
        evt.type              = FlowEventType::End;
        evt.state             = it->second;
        evt.event_timestamp_us = now_us;
        evt.reason            = ExpireReason::IdleTimeout;
        emit(std::move(evt));
        metrics_.flows_active().Decrement();
        metrics_.flows_expired(ExpireReason::IdleTimeout).Increment();
        it = flows_.erase(it);
    }
}

} // namespace packetpipe
