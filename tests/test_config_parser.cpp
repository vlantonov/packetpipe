#include "config/config_parser.hpp"
#include <gtest/gtest.h>
#include <stdexcept>
#include <vector>

using namespace packetpipe;

namespace {

AppConfig parse(std::vector<const char*> args) {
    args.insert(args.begin(), "packetpipe"); // argv[0]
    return ConfigParser::parse(static_cast<int>(args.size()),
                               const_cast<char**>(args.data()));
}

} // namespace

TEST(ConfigParserTest, PcapOnlyIsValid) {
    auto cfg = parse({"--pcap", "/tmp/test.pcap"});
    EXPECT_EQ(cfg.pcap_file, "/tmp/test.pcap");
    EXPECT_TRUE(cfg.iface.empty());
}

TEST(ConfigParserTest, IfaceOnlyIsValid) {
    auto cfg = parse({"--iface", "eth0"});
    EXPECT_EQ(cfg.iface, "eth0");
    EXPECT_TRUE(cfg.pcap_file.empty());
}

TEST(ConfigParserTest, BothPcapAndIfaceFails) {
    EXPECT_THROW(parse({"--pcap", "a.pcap", "--iface", "eth0"}),
                 std::invalid_argument);
}

TEST(ConfigParserTest, NeitherPcapNorIfaceFails) {
    EXPECT_THROW(parse({}), std::invalid_argument);
}

TEST(ConfigParserTest, DefaultsAreApplied) {
    auto cfg = parse({"--pcap", "x.pcap"});
    EXPECT_EQ(cfg.flow_timeout_sec,  60);
    EXPECT_EQ(cfg.max_flows,         100'000);
    EXPECT_EQ(cfg.stats_interval_sec, 30);
    EXPECT_EQ(cfg.kafka_brokers,      "localhost:9092");
    EXPECT_EQ(cfg.kafka_topic,        "packetpipe.flows");
    EXPECT_EQ(cfg.schema_registry_url,"http://localhost:8081");
    EXPECT_EQ(cfg.sr_buffer_size,     1'000);
    EXPECT_EQ(cfg.metrics_port,       9090u);
    EXPECT_EQ(cfg.log_level,          "info");
}

TEST(ConfigParserTest, BpfFilterIsPassedThrough) {
    auto cfg = parse({"--pcap", "x.pcap", "--filter", "tcp port 80"});
    EXPECT_EQ(cfg.bpf_filter, "tcp port 80");
}

TEST(ConfigParserTest, FlowTimeoutOverride) {
    auto cfg = parse({"--pcap", "x.pcap", "--flow-timeout", "120"});
    EXPECT_EQ(cfg.flow_timeout_sec, 120);
}

TEST(ConfigParserTest, InvalidLogLevelFails) {
    EXPECT_THROW(parse({"--pcap", "x.pcap", "--log-level", "verbose"}),
                 std::invalid_argument);
}

TEST(ConfigParserTest, MetricsPortOverride) {
    auto cfg = parse({"--pcap", "x.pcap", "--metrics-port", "8080"});
    EXPECT_EQ(cfg.metrics_port, 8080u);
}

TEST(ConfigParserTest, UnknownFlagFails) {
    EXPECT_THROW(parse({"--pcap", "x.pcap", "--unknown-flag"}),
                 std::invalid_argument);
}
