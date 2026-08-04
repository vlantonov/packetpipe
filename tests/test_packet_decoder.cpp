#include "packets/packet_decoder.hpp"
#include <gtest/gtest.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <array>
#include <cstring>
#include <sys/time.h>

using namespace packetpipe;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

timeval ts_zero() { return {0, 0}; }

// Build a minimal DLT_EN10MB Ethernet + IPv4 + TCP frame.
// Returns a buffer and its length.
std::pair<std::array<uint8_t, 256>, uint32_t>
make_eth_ipv4_tcp(uint32_t src_ip, uint32_t dst_ip,
                  uint16_t src_port, uint16_t dst_port,
                  bool fin = false, bool rst = false) {
    std::array<uint8_t, 256> buf{};
    uint32_t off = 0;

    // Ethernet header (14 bytes): dst mac, src mac, ethertype=0x0800
    buf[12] = 0x08; buf[13] = 0x00;
    off = 14;

    // IPv4 header
    auto* ip4 = reinterpret_cast<struct ip*>(buf.data() + off);
    ip4->ip_v   = 4;
    ip4->ip_hl  = 5; // 20 bytes
    ip4->ip_ttl = 64;
    ip4->ip_p   = IPPROTO_TCP;
    ip4->ip_len = htons(20 + 20); // ip hdr + tcp hdr
    ip4->ip_src.s_addr = htonl(src_ip);
    ip4->ip_dst.s_addr = htonl(dst_ip);
    off += 20;

    // TCP header
    auto* tcp = reinterpret_cast<struct tcphdr*>(buf.data() + off);
    tcp->th_sport  = htons(src_port);
    tcp->th_dport  = htons(dst_port);
    tcp->th_off    = 5; // 20-byte header
    tcp->th_flags  = (fin ? TH_FIN : 0) | (rst ? TH_RST : 0);
    off += 20;

    return {buf, off};
}

std::pair<std::array<uint8_t, 256>, uint32_t>
make_eth_ipv4_udp(uint32_t src_ip, uint32_t dst_ip,
                  uint16_t src_port, uint16_t dst_port) {
    std::array<uint8_t, 256> buf{};
    uint32_t off = 0;

    buf[12] = 0x08; buf[13] = 0x00;
    off = 14;

    auto* ip4 = reinterpret_cast<struct ip*>(buf.data() + off);
    ip4->ip_v   = 4;
    ip4->ip_hl  = 5;
    ip4->ip_ttl = 64;
    ip4->ip_p   = IPPROTO_UDP;
    ip4->ip_len = htons(20 + 8);
    ip4->ip_src.s_addr = htonl(src_ip);
    ip4->ip_dst.s_addr = htonl(dst_ip);
    off += 20;

    auto* udp = reinterpret_cast<struct udphdr*>(buf.data() + off);
    udp->uh_sport = htons(src_port);
    udp->uh_dport = htons(dst_port);
    udp->uh_ulen  = htons(8);
    off += 8;

    return {buf, off};
}

} // namespace

// ---------------------------------------------------------------------------
// Nullopt (decode failure) paths – these are what packets_dropped tests
// ---------------------------------------------------------------------------

TEST(PacketDecoderTest, NullDataReturnsNullopt) {
    EXPECT_FALSE(PacketDecoder::decode(nullptr, 64, ts_zero()).has_value());
}

TEST(PacketDecoderTest, TooShortForEthernetHeaderReturnsNullopt) {
    std::array<uint8_t, 10> buf{};
    EXPECT_FALSE(PacketDecoder::decode(buf.data(), 10, ts_zero()).has_value());
}

TEST(PacketDecoderTest, NonIpEthertypeReturnsNullopt) {
    // ARP ethertype 0x0806 → not IP
    std::array<uint8_t, 64> buf{};
    buf[12] = 0x08; buf[13] = 0x06;
    EXPECT_FALSE(PacketDecoder::decode(buf.data(), 64, ts_zero()).has_value());
}

TEST(PacketDecoderTest, UnknownTransportProtocolReturnsNullopt) {
    // Build IP frame with protocol = IPPROTO_GRE (47) – not handled
    auto [buf, len] = make_eth_ipv4_tcp(0xC0A80101, 0x0A000001, 1000, 80);
    buf[14 + 9] = 47; // overwrite ip_p with GRE
    EXPECT_FALSE(PacketDecoder::decode(buf.data(), len, ts_zero()).has_value());
}

TEST(PacketDecoderTest, TruncatedTcpHeaderReturnsNullopt) {
    auto [buf, len] = make_eth_ipv4_tcp(0xC0A80101, 0x0A000001, 1000, 80);
    // Give only enough bytes for Eth + IP, not TCP
    EXPECT_FALSE(PacketDecoder::decode(buf.data(), 14 + 20 + 4, ts_zero()).has_value());
}

TEST(PacketDecoderTest, TruncatedUdpHeaderReturnsNullopt) {
    auto [buf, len] = make_eth_ipv4_udp(0xC0A80101, 0x0A000001, 5000, 53);
    // Provide less than a full UDP header after Eth + IP
    EXPECT_FALSE(PacketDecoder::decode(buf.data(), 14 + 20 + 3, ts_zero()).has_value());
}

TEST(PacketDecoderTest, BadIpVersionFieldReturnsNullopt) {
    auto [buf, len] = make_eth_ipv4_tcp(0xC0A80101, 0x0A000001, 1000, 80);
    // Corrupt: set version field to 5
    buf[14] = (buf[14] & 0x0F) | (5 << 4);
    EXPECT_FALSE(PacketDecoder::decode(buf.data(), len, ts_zero()).has_value());
}

// ---------------------------------------------------------------------------
// Successful decode paths
// ---------------------------------------------------------------------------

TEST(PacketDecoderTest, ValidTcpPacketDecodes) {
    auto [buf, len] = make_eth_ipv4_tcp(0xC0A80101, 0x0A000001, 54321, 80);
    auto pkt = PacketDecoder::decode(buf.data(), len, ts_zero());
    ASSERT_TRUE(pkt.has_value());
    EXPECT_EQ(pkt->src_port,  54321u);
    EXPECT_EQ(pkt->dst_port,  80u);
    EXPECT_EQ(pkt->protocol,  static_cast<uint8_t>(IPPROTO_TCP));
    EXPECT_FALSE(pkt->tcp_fin);
    EXPECT_FALSE(pkt->tcp_rst);
    EXPECT_FALSE(pkt->is_ipv6);
}

TEST(PacketDecoderTest, TcpFinFlagSet) {
    auto [buf, len] = make_eth_ipv4_tcp(0xC0A80101, 0x0A000001, 1000, 80, /*fin=*/true);
    auto pkt = PacketDecoder::decode(buf.data(), len, ts_zero());
    ASSERT_TRUE(pkt.has_value());
    EXPECT_TRUE(pkt->tcp_fin);
    EXPECT_FALSE(pkt->tcp_rst);
}

TEST(PacketDecoderTest, TcpRstFlagSet) {
    auto [buf, len] = make_eth_ipv4_tcp(0xC0A80101, 0x0A000001, 1000, 80, false, /*rst=*/true);
    auto pkt = PacketDecoder::decode(buf.data(), len, ts_zero());
    ASSERT_TRUE(pkt.has_value());
    EXPECT_TRUE(pkt->tcp_rst);
}

TEST(PacketDecoderTest, ValidUdpPacketDecodes) {
    auto [buf, len] = make_eth_ipv4_udp(0xC0A80101, 0x0A000001, 5000, 53);
    auto pkt = PacketDecoder::decode(buf.data(), len, ts_zero());
    ASSERT_TRUE(pkt.has_value());
    EXPECT_EQ(pkt->src_port, 5000u);
    EXPECT_EQ(pkt->dst_port, 53u);
    EXPECT_EQ(pkt->protocol, static_cast<uint8_t>(IPPROTO_UDP));
}

TEST(PacketDecoderTest, Ipv4AddressMappedToIpv4Mapped) {
    // 192.168.1.1 → ::ffff:192.168.1.1
    auto [buf, len] = make_eth_ipv4_tcp(0xC0A80101, 0x0A000001, 1000, 80);
    auto pkt = PacketDecoder::decode(buf.data(), len, ts_zero());
    ASSERT_TRUE(pkt.has_value());
    EXPECT_FALSE(pkt->is_ipv6);
    // Bytes 10-11 must be 0xFF 0xFF (IPv4-mapped)
    EXPECT_EQ(pkt->src_ip[10], 0xFF);
    EXPECT_EQ(pkt->src_ip[11], 0xFF);
    EXPECT_EQ(pkt->src_ip[12], 192);
    EXPECT_EQ(pkt->src_ip[15], 1);
}

TEST(PacketDecoderTest, TimestampPreserved) {
    auto [buf, len] = make_eth_ipv4_tcp(0xC0A80101, 0x0A000001, 1000, 80);
    timeval ts{1700000000L, 500000L};
    auto pkt = PacketDecoder::decode(buf.data(), len, ts);
    ASSERT_TRUE(pkt.has_value());
    const int64_t expected = 1700000000LL * 1'000'000LL + 500'000LL;
    EXPECT_EQ(pkt->timestamp_us, expected);
}

TEST(PacketDecoderTest, VlanTaggedFrameDecodes) {
    // Build: Eth (12) + ethertype=0x8100 (VLAN) + 2-byte TCI + inner ethertype=0x0800 + IPv4+TCP
    std::array<uint8_t, 256> buf{};
    buf[12] = 0x81; buf[13] = 0x00; // VLAN
    buf[14] = 0x00; buf[15] = 0x64; // TCI (VLAN ID 100)
    buf[16] = 0x08; buf[17] = 0x00; // inner ethertype = IPv4
    uint32_t off = 18;

    auto* ip4 = reinterpret_cast<struct ip*>(buf.data() + off);
    ip4->ip_v  = 4; ip4->ip_hl = 5; ip4->ip_ttl = 64;
    ip4->ip_p  = IPPROTO_TCP;
    ip4->ip_len = htons(20 + 20);
    ip4->ip_src.s_addr = htonl(0xC0A80101);
    ip4->ip_dst.s_addr = htonl(0x0A000001);
    off += 20;

    auto* tcp = reinterpret_cast<struct tcphdr*>(buf.data() + off);
    tcp->th_sport = htons(9999); tcp->th_dport = htons(443);
    tcp->th_off = 5;
    off += 20;

    auto pkt = PacketDecoder::decode(buf.data(), off, ts_zero());
    ASSERT_TRUE(pkt.has_value());
    EXPECT_EQ(pkt->src_port, 9999u);
    EXPECT_EQ(pkt->dst_port, 443u);
}

TEST(PacketDecoderTest, IcmpTypeCodeEncodedInSrcPort) {
    // ICMP echo request type=8 code=0 → src_port = (8 << 8) | 0 = 2048
    std::array<uint8_t, 256> buf{};
    buf[12] = 0x08; buf[13] = 0x00;
    uint32_t off = 14;
    auto* ip4 = reinterpret_cast<struct ip*>(buf.data() + off);
    ip4->ip_v = 4; ip4->ip_hl = 5; ip4->ip_ttl = 64;
    ip4->ip_p = IPPROTO_ICMP;
    ip4->ip_len = htons(20 + 8);
    ip4->ip_src.s_addr = htonl(0xC0A80101);
    ip4->ip_dst.s_addr = htonl(0x08080808);
    off += 20;
    buf[off]     = 8; // type = echo request
    buf[off + 1] = 0; // code
    off += 8;
    auto pkt = PacketDecoder::decode(buf.data(), off, ts_zero());
    ASSERT_TRUE(pkt.has_value());
    EXPECT_EQ(pkt->src_port, 2048u); // (8 << 8) | 0
    EXPECT_EQ(pkt->protocol, static_cast<uint8_t>(IPPROTO_ICMP));
}
