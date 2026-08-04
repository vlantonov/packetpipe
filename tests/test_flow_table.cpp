#include "flowtable/flow_table.hpp"
#include "config/app_config.hpp"
#include "metrics/imetrics_registry.hpp"
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>
#include <vector>
#include <cstring>

using namespace packetpipe;

// ── Stub metrics (no Prometheus dependency in unit tests) ─────────────────────

namespace {

struct StubCounter {
    double value{0.0};
    void Increment(double v = 1.0) { value += v; }
    void Reset() { value = 0.0; }
};

struct StubGauge {
    double value{0.0};
    void Set(double v) { value = v; }
    void Increment(double v = 1.0) { value += v; }
    void Decrement(double v = 1.0) { value -= v; }
};

} // namespace

// We can't instantiate MetricsRegistry without a live Prometheus port, so we
// write a minimal stub that satisfies IMetricsRegistry.

class StubMetrics : public IMetricsRegistry {
public:
    // Trick: return fake prometheus:: objects.
    // We create a real (but un-exposed) registry + counters via the family builders.
    // For pure unit tests this avoids any port binding.
    StubMetrics() {
        reg_ = std::make_shared<prometheus::Registry>();
        auto& cr_fam = prometheus::BuildCounter().Name("p").Help("").Register(*reg_);
        auto& gr_fam = prometheus::BuildGauge().Name("g").Help("").Register(*reg_);
        pkts_rcvd_      = &cr_fam.Add({});
        pkts_drpd_      = &cr_fam.Add({{"l","d"}});
        flows_crtd_     = &cr_fam.Add({{"l","c"}});
        flows_exp_idle_ = &cr_fam.Add({{"l","i"}});
        flows_exp_tcp_  = &cr_fam.Add({{"l","t"}});
        kafka_prod_     = &cr_fam.Add({{"l","kp"}});
        kafka_err_      = &cr_fam.Add({{"l","ke"}});
        avro_err_       = &cr_fam.Add({{"l","ae"}});
        sr_retry_       = &cr_fam.Add({{"l","sr"}});
        flows_active_   = &gr_fam.Add({});
    }

    prometheus::Counter& packets_received()            override { return *pkts_rcvd_; }
    prometheus::Counter& packets_dropped()             override { return *pkts_drpd_; }
    prometheus::Gauge&   flows_active()                override { return *flows_active_; }
    prometheus::Counter& flows_created()               override { return *flows_crtd_; }
    prometheus::Counter& flows_expired(ExpireReason r) override {
        return (r == ExpireReason::IdleTimeout) ? *flows_exp_idle_ : *flows_exp_tcp_;
    }
    prometheus::Counter& kafka_messages_produced()     override { return *kafka_prod_; }
    prometheus::Counter& kafka_delivery_errors()       override { return *kafka_err_; }
    prometheus::Counter& avro_serialization_errors()   override { return *avro_err_; }
    prometheus::Counter& schema_registry_retries()     override { return *sr_retry_; }

private:
    std::shared_ptr<prometheus::Registry> reg_;
    prometheus::Counter *pkts_rcvd_, *pkts_drpd_, *flows_crtd_,
                        *flows_exp_idle_, *flows_exp_tcp_,
                        *kafka_prod_, *kafka_err_, *avro_err_, *sr_retry_;
    prometheus::Gauge* flows_active_;
};

namespace {

AppConfig make_cfg(int timeout_sec = 60, int max_flows = 100000) {
    AppConfig cfg;
    cfg.pcap_file         = "test.pcap"; // satisfy validation
    cfg.flow_timeout_sec  = timeout_sec;
    cfg.max_flows         = max_flows;
    cfg.stats_interval_sec = 30;
    return cfg;
}

ParsedPacket make_tcp_pkt(uint8_t src3, uint8_t dst3,
                          uint16_t sp, uint16_t dp,
                          bool fin = false, bool rst = false) {
    ParsedPacket p;
    p.src_ip.fill(0); p.src_ip[10] = 0xFF; p.src_ip[11] = 0xFF;
    p.src_ip[12] = 192; p.src_ip[13] = 168; p.src_ip[14] = 1; p.src_ip[15] = src3;
    p.dst_ip.fill(0); p.dst_ip[10] = 0xFF; p.dst_ip[11] = 0xFF;
    p.dst_ip[12] = 10;  p.dst_ip[13] = 0;   p.dst_ip[14] = 0; p.dst_ip[15] = dst3;
    p.src_port   = sp;
    p.dst_port   = dp;
    p.protocol   = IPPROTO_TCP;
    p.wire_length = 64;
    p.timestamp_us = 1'000'000LL;
    p.tcp_fin    = fin;
    p.tcp_rst    = rst;
    return p;
}

} // namespace

TEST(FlowTableTest, NewFlowEmitsStartEvent) {
    StubMetrics metrics;
    std::vector<FlowEvent> events;
    AppConfig cfg = make_cfg();
    FlowTable table(cfg, metrics, [&](FlowEvent e) { events.push_back(e); return true; });

    table.process(make_tcp_pkt(1, 1, 1000, 80));
    ASSERT_EQ(events.size(), 1u);
    EXPECT_EQ(events[0].type, FlowEventType::Start);
    EXPECT_EQ(events[0].state.packet_count, 1u);
}

TEST(FlowTableTest, SecondPacketOnSameFlowNoExtraEvent) {
    StubMetrics metrics;
    std::vector<FlowEvent> events;
    FlowTable table(make_cfg(), metrics, [&](FlowEvent e) { events.push_back(e); return true; });

    auto pkt = make_tcp_pkt(1, 1, 1000, 80);
    table.process(pkt);
    pkt.timestamp_us += 1000;
    table.process(pkt);
    EXPECT_EQ(events.size(), 1u); // only the Start
}

TEST(FlowTableTest, TcpFinEmitsEndWithTeardownReason) {
    StubMetrics metrics;
    std::vector<FlowEvent> events;
    FlowTable table(make_cfg(), metrics, [&](FlowEvent e) { events.push_back(e); return true; });

    table.process(make_tcp_pkt(1, 1, 1000, 80));
    table.process(make_tcp_pkt(1, 1, 1000, 80, /*fin=*/true));

    ASSERT_EQ(events.size(), 2u);
    EXPECT_EQ(events[1].type,   FlowEventType::End);
    EXPECT_EQ(events[1].reason, ExpireReason::TcpTeardown);
}

TEST(FlowTableTest, TcpRstEmitsEndWithTeardownReason) {
    StubMetrics metrics;
    std::vector<FlowEvent> events;
    FlowTable table(make_cfg(), metrics, [&](FlowEvent e) { events.push_back(e); return true; });

    table.process(make_tcp_pkt(1, 1, 1000, 80));
    table.process(make_tcp_pkt(1, 1, 1000, 80, false, /*rst=*/true));

    ASSERT_EQ(events.size(), 2u);
    EXPECT_EQ(events[1].reason, ExpireReason::TcpTeardown);
}

TEST(FlowTableTest, IdleTimeoutEmitsEndWithIdleReason) {
    StubMetrics metrics;
    std::vector<FlowEvent> events;
    FlowTable table(make_cfg(/*timeout=*/10), metrics,
                    [&](FlowEvent e) { events.push_back(e); return true; });

    auto pkt = make_tcp_pkt(1, 1, 1000, 80);
    table.process(pkt);

    // Sweep at now = packet_ts + 11 seconds → flow should expire
    const int64_t now = pkt.timestamp_us + 11'000'000LL;
    table.timer_sweep(now);

    ASSERT_EQ(events.size(), 2u);
    EXPECT_EQ(events[1].type,   FlowEventType::End);
    EXPECT_EQ(events[1].reason, ExpireReason::IdleTimeout);
}

TEST(FlowTableTest, HeartbeatEmitsStatsForAllActiveFlows) {
    StubMetrics metrics;
    std::vector<FlowEvent> events;
    AppConfig cfg = make_cfg();
    cfg.stats_interval_sec = 1;
    FlowTable table(cfg, metrics, [&](FlowEvent e) { events.push_back(e); return true; });

    table.process(make_tcp_pkt(1, 1, 1000, 80));
    table.process(make_tcp_pkt(2, 2, 2000, 80));
    events.clear(); // discard Start events

    // Sweep 2 seconds after first packet → heartbeat fires
    table.timer_sweep(1'000'000LL + 2'000'000LL);

    const long stats_count = std::count_if(events.begin(), events.end(),
        [](const FlowEvent& e) { return e.type == FlowEventType::Stats; });
    EXPECT_EQ(stats_count, 2); // one per active flow
}

TEST(FlowTableTest, MaxFlowsCapDropsNewFlows) {
    StubMetrics metrics;
    std::vector<FlowEvent> events;
    FlowTable table(make_cfg(60, /*max_flows=*/2), metrics,
                    [&](FlowEvent e) { events.push_back(e); return true; });

    table.process(make_tcp_pkt(1, 1, 1000, 80));
    table.process(make_tcp_pkt(2, 2, 2000, 80));
    table.process(make_tcp_pkt(3, 3, 3000, 80)); // should be dropped

    const long starts = std::count_if(events.begin(), events.end(),
        [](const FlowEvent& e) { return e.type == FlowEventType::Start; });
    EXPECT_EQ(starts, 2); // third flow was dropped
}

TEST(FlowTableTest, DrainAllEmitsEndForAllFlows) {
    StubMetrics metrics;
    std::vector<FlowEvent> events;
    FlowTable table(make_cfg(), metrics, [&](FlowEvent e) { events.push_back(e); return true; });

    table.process(make_tcp_pkt(1, 1, 1000, 80));
    table.process(make_tcp_pkt(2, 2, 2000, 80));
    events.clear();

    table.drain_all(2'000'000LL);
    EXPECT_EQ(events.size(), 2u);
    for (const auto& e : events) EXPECT_EQ(e.type, FlowEventType::End);
}
