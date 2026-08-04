#include "events/flow_key.hpp"
#include <gtest/gtest.h>
#include <cstring>

using namespace packetpipe;

namespace {

FlowKey make_ipv4_key(const uint8_t* src4, const uint8_t* dst4,
                      uint16_t sp, uint16_t dp, uint8_t proto) {
    FlowKey k;
    k.src_ip.fill(0); k.src_ip[10] = 0xFF; k.src_ip[11] = 0xFF;
    k.dst_ip.fill(0); k.dst_ip[10] = 0xFF; k.dst_ip[11] = 0xFF;
    std::memcpy(k.src_ip.data() + 12, src4, 4);
    std::memcpy(k.dst_ip.data() + 12, dst4, 4);
    k.src_port = sp; k.dst_port = dp; k.protocol = proto;
    return k;
}

} // namespace

TEST(FlowKeyTest, SameTupleIsEqual) {
    const uint8_t a[] = {192, 168, 1, 1};
    const uint8_t b[] = {10,  0,   0, 1};
    auto k1 = make_ipv4_key(a, b, 1234, 80, IPPROTO_TCP);
    auto k2 = make_ipv4_key(a, b, 1234, 80, IPPROTO_TCP);
    EXPECT_EQ(k1, k2);
}

TEST(FlowKeyTest, DifferentSrcPortIsNotEqual) {
    const uint8_t a[] = {192, 168, 1, 1};
    const uint8_t b[] = {10,  0,   0, 1};
    auto k1 = make_ipv4_key(a, b, 1234, 80, IPPROTO_TCP);
    auto k2 = make_ipv4_key(a, b, 9999, 80, IPPROTO_TCP);
    EXPECT_NE(k1, k2);
}

TEST(FlowKeyTest, DifferentProtocolIsNotEqual) {
    const uint8_t a[] = {192, 168, 1, 1};
    const uint8_t b[] = {10,  0,   0, 1};
    auto k1 = make_ipv4_key(a, b, 1234, 80, IPPROTO_TCP);
    auto k2 = make_ipv4_key(a, b, 1234, 80, IPPROTO_UDP);
    EXPECT_NE(k1, k2);
}

TEST(FlowKeyTest, HashStabilityForSameTuple) {
    const uint8_t a[] = {192, 168, 1, 1};
    const uint8_t b[] = {10,  0,   0, 1};
    auto k1 = make_ipv4_key(a, b, 1234, 80, IPPROTO_TCP);
    auto k2 = make_ipv4_key(a, b, 1234, 80, IPPROTO_TCP);
    FlowKeyHash hasher;
    EXPECT_EQ(hasher(k1), hasher(k2));
}

TEST(FlowKeyTest, DifferentTuplesDifferentHash) {
    const uint8_t a[] = {192, 168, 1, 1};
    const uint8_t b[] = {10,  0,   0, 1};
    const uint8_t c[] = {10,  0,   0, 2};
    auto k1 = make_ipv4_key(a, b, 1234, 80, IPPROTO_TCP);
    auto k2 = make_ipv4_key(a, c, 1234, 80, IPPROTO_TCP); // different dst IP
    FlowKeyHash hasher;
    EXPECT_NE(hasher(k1), hasher(k2));
}

TEST(FlowKeyTest, IcmpTypeCodeInPortFields) {
    // ICMP type=8 (echo request), code=0 → src_port = (8<<8)|0 = 0x0800
    FlowKey k;
    k.src_ip.fill(0); k.src_ip[10] = 0xFF; k.src_ip[11] = 0xFF;
    k.dst_ip.fill(0); k.dst_ip[10] = 0xFF; k.dst_ip[11] = 0xFF;
    k.src_port = static_cast<uint16_t>((8u << 8) | 0u); // ICMP echo
    k.dst_port = 0;
    k.protocol = IPPROTO_ICMP;
    EXPECT_EQ(k.src_port, 0x0800u);
}

TEST(FlowKeyTest, Ipv6KeyNotEqualToIpv4Mapped) {
    // Two keys with different 16-byte address content are not equal
    FlowKey ipv4_key{};
    ipv4_key.src_ip[10] = 0xFF; ipv4_key.src_ip[11] = 0xFF;
    ipv4_key.src_ip[12] = 192;  ipv4_key.src_ip[13] = 168;
    ipv4_key.src_ip[14] = 1;    ipv4_key.src_ip[15] = 1;

    FlowKey ipv6_key{};
    ipv6_key.src_ip[0] = 0x20; ipv6_key.src_ip[1] = 0x01; // 2001:...

    EXPECT_NE(ipv4_key, ipv6_key);
}
