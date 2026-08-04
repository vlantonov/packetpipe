#include "avro/avro_serializer.hpp"
#include "events/flow_key.hpp"
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <cstring>
#include <string>

using namespace packetpipe;

namespace {

FlowKey make_ipv4_key(uint8_t s1, uint8_t s2, uint8_t s3, uint8_t s4,
                      uint8_t d1, uint8_t d2, uint8_t d3, uint8_t d4,
                      uint16_t sp, uint16_t dp, uint8_t proto) {
    FlowKey k;
    k.src_ip.fill(0); k.src_ip[10] = 0xFF; k.src_ip[11] = 0xFF;
    k.src_ip[12] = s1; k.src_ip[13] = s2; k.src_ip[14] = s3; k.src_ip[15] = s4;
    k.dst_ip.fill(0); k.dst_ip[10] = 0xFF; k.dst_ip[11] = 0xFF;
    k.dst_ip[12] = d1; k.dst_ip[13] = d2; k.dst_ip[14] = d3; k.dst_ip[15] = d4;
    k.src_port = sp; k.dst_port = dp; k.protocol = proto;
    return k;
}

} // namespace

TEST(KafkaKeyTest, Ipv4KeyFormat) {
    // 192.168.1.10:54321 → 10.0.0.1:80 / TCP (proto 6)
    auto k = make_ipv4_key(192,168,1,10, 10,0,0,1, 54321, 80, IPPROTO_TCP);
    const std::string key = AvroSerializer::flow_id(k);
    EXPECT_EQ(key, "192.168.1.10:54321-10.0.0.1:80/6");
}

TEST(KafkaKeyTest, Ipv6KeyFormatWithBrackets) {
    FlowKey k;
    k.src_ip.fill(0); k.src_ip[0] = 0x20; k.src_ip[1] = 0x01; // 2001:...
    k.dst_ip.fill(0); k.dst_ip[0] = 0x20; k.dst_ip[1] = 0x01;
    k.src_port = 1234; k.dst_port = 80; k.protocol = IPPROTO_TCP;
    const std::string key = AvroSerializer::flow_id(k);
    EXPECT_TRUE(key.find('[') != std::string::npos) << "IPv6 key must use bracket notation";
    EXPECT_TRUE(key.find(']') != std::string::npos);
}

TEST(KafkaKeyTest, IcmpKeyFormat) {
    // ICMP echo request: type=8, code=0 → src_port = 0x0800
    auto k = make_ipv4_key(192,168,1,1, 8,8,8,8, 0x0800, 0, IPPROTO_ICMP);
    const std::string key = AvroSerializer::flow_id(k);
    // Key should contain the encoded port 2048
    EXPECT_NE(key.find("2048"), std::string::npos);
}

TEST(KafkaKeyTest, KeyIsDeterministicForSameTuple) {
    auto k1 = make_ipv4_key(10,0,0,1, 10,0,0,2, 5000, 443, IPPROTO_TCP);
    auto k2 = make_ipv4_key(10,0,0,1, 10,0,0,2, 5000, 443, IPPROTO_TCP);
    EXPECT_EQ(AvroSerializer::flow_id(k1), AvroSerializer::flow_id(k2));
}

TEST(KafkaKeyTest, DifferentTuplesDifferentKeys) {
    auto k1 = make_ipv4_key(10,0,0,1, 10,0,0,2, 5000, 443, IPPROTO_TCP);
    auto k2 = make_ipv4_key(10,0,0,1, 10,0,0,2, 5001, 443, IPPROTO_TCP);
    EXPECT_NE(AvroSerializer::flow_id(k1), AvroSerializer::flow_id(k2));
}
