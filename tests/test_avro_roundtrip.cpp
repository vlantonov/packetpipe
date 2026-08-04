#include "avro/avro_serializer.hpp"
#include "avro/schema_registry_client.hpp"
#include <fmt/format.h>
#include <avro/Decoder.hh>
#include <avro/Generic.hh>
#include <avro/Stream.hh>
#include <gtest/gtest.h>
#include <cstring>
#include <fstream>
#include <netinet/in.h>
#include <sstream>

using namespace packetpipe;

namespace {

// TEST_SCHEMA_PATH is injected by CMake
#ifndef TEST_SCHEMA_PATH
#define TEST_SCHEMA_PATH "schemas/FlowEvent.avsc"
#endif

FlowEvent make_start_event() {
    FlowEvent e;
    e.type = FlowEventType::Start;
    e.state.key.src_ip.fill(0);
    e.state.key.src_ip[10] = 0xFF; e.state.key.src_ip[11] = 0xFF;
    e.state.key.src_ip[12] = 192;  e.state.key.src_ip[13] = 168;
    e.state.key.src_ip[14] = 1;    e.state.key.src_ip[15] = 10;
    e.state.key.dst_ip.fill(0);
    e.state.key.dst_ip[10] = 0xFF; e.state.key.dst_ip[11] = 0xFF;
    e.state.key.dst_ip[12] = 10;   e.state.key.dst_ip[13] = 0;
    e.state.key.dst_ip[14] = 0;    e.state.key.dst_ip[15] = 1;
    e.state.key.src_port  = 54321;
    e.state.key.dst_port  = 80;
    e.state.key.protocol  = IPPROTO_TCP;
    e.state.packet_count  = 7;
    e.state.byte_count    = 4096;
    e.state.start_us      = 1'700'000'000'000'000LL;
    e.event_timestamp_us  = 1'700'000'001'000'000LL;
    return e;
}

} // namespace

class AvroRoundtripTest : public ::testing::Test {
protected:
    void SetUp() override {
        sr_client_ = std::make_unique<SchemaRegistryClient>(42); // fake ID
        serializer_ = std::make_unique<AvroSerializer>(TEST_SCHEMA_PATH, *sr_client_);

        // Load schema for decoding assertions
        std::ifstream ifs(TEST_SCHEMA_PATH);
        ASSERT_TRUE(ifs.good()) << "Cannot open schema file: " << TEST_SCHEMA_PATH;
        avro::compileJsonSchema(ifs, schema_);
    }

    std::unique_ptr<SchemaRegistryClient> sr_client_;
    std::unique_ptr<AvroSerializer>       serializer_;
    avro::ValidSchema                     schema_;
};

TEST_F(AvroRoundtripTest, WireFormatMagicByte) {
    auto bytes = serializer_->serialize(make_start_event());
    ASSERT_GE(bytes.size(), 5u);
    EXPECT_EQ(bytes[0], 0x00u) << "First byte must be Confluent magic byte 0x00";
}

TEST_F(AvroRoundtripTest, WireFormatSchemaIdBigEndian) {
    auto bytes = serializer_->serialize(make_start_event());
    ASSERT_GE(bytes.size(), 5u);
    const uint32_t schema_id =
        (static_cast<uint32_t>(bytes[1]) << 24) |
        (static_cast<uint32_t>(bytes[2]) << 16) |
        (static_cast<uint32_t>(bytes[3]) <<  8) |
        (static_cast<uint32_t>(bytes[4]));
    EXPECT_EQ(schema_id, 42u);
}

TEST_F(AvroRoundtripTest, RoundtripFlowStart) {
    const FlowEvent orig = make_start_event();
    const auto wire = serializer_->serialize(orig);

    // Decode the Avro payload (skip 5-byte wire-format header)
    const std::string payload(wire.begin() + 5, wire.end());
    std::istringstream iss(payload);
    auto in  = avro::istreamInputStream(iss);
    auto dec = avro::binaryDecoder();
    dec->init(*in);

    avro::GenericDatum datum(schema_);
    avro::decode(*dec, datum);

    const auto& rec = datum.value<avro::GenericRecord>();

    const std::string& event_type =
        rec.fieldAt(1).value<avro::GenericEnum>().symbol();
    EXPECT_EQ(event_type, "FLOW_START");

    const std::string& flow_id = rec.fieldAt(2).value<std::string>();
    EXPECT_EQ(flow_id, AvroSerializer::flow_id(orig.state.key));

    EXPECT_EQ(rec.fieldAt(9).value<int64_t>(), orig.event_timestamp_us);
    EXPECT_EQ(rec.fieldAt(10).value<int64_t>(), static_cast<int64_t>(orig.state.packet_count));
    EXPECT_EQ(rec.fieldAt(11).value<int64_t>(), static_cast<int64_t>(orig.state.byte_count));
}

TEST_F(AvroRoundtripTest, RoundtripFlowEndWithExpireReason) {
    FlowEvent evt = make_start_event();
    evt.type   = FlowEventType::End;
    evt.reason = ExpireReason::IdleTimeout;

    const auto wire = serializer_->serialize(evt);
    const std::string payload(wire.begin() + 5, wire.end());
    std::istringstream iss(payload);
    auto in  = avro::istreamInputStream(iss);
    auto dec = avro::binaryDecoder();
    dec->init(*in);

    avro::GenericDatum datum(schema_);
    avro::decode(*dec, datum);
    const auto& rec = datum.value<avro::GenericRecord>();

    EXPECT_EQ(rec.fieldAt(1).value<avro::GenericEnum>().symbol(), "FLOW_END");

    // expire_reason union should be non-null
    const auto& reason_union = rec.fieldAt(12).value<avro::GenericUnion>();
    EXPECT_EQ(reason_union.currentBranch(), 1u); // string branch
    EXPECT_EQ(reason_union.datum().value<std::string>(), "idle_timeout");
}

TEST_F(AvroRoundtripTest, RoundtripFlowStats) {
    FlowEvent evt = make_start_event();
    evt.type = FlowEventType::Stats;

    const auto wire = serializer_->serialize(evt);
    const std::string payload(wire.begin() + 5, wire.end());
    std::istringstream iss(payload);
    auto in  = avro::istreamInputStream(iss);
    auto dec = avro::binaryDecoder();
    dec->init(*in);

    avro::GenericDatum datum(schema_);
    avro::decode(*dec, datum);
    const auto& rec = datum.value<avro::GenericRecord>();
    EXPECT_EQ(rec.fieldAt(1).value<avro::GenericEnum>().symbol(), "FLOW_STATS");
}
