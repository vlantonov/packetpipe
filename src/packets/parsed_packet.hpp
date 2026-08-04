#pragma once
#include <array>
#include <cstdint>

namespace packetpipe {

/// Decoded layer-2/3/4 packet with uniform IPv4-mapped-IPv6 addresses.
/// IPv4 addresses are stored as ::ffff:a.b.c.d (first 10 bytes = 0, bytes 10-11 = 0xFF).
struct ParsedPacket {
    std::array<uint8_t, 16> src_ip{};
    std::array<uint8_t, 16> dst_ip{};
    uint16_t src_port{0};    ///< ICMP: (type << 8) | code
    uint16_t dst_port{0};
    uint8_t  protocol{0};    ///< IPPROTO_TCP / UDP / ICMP / ICMPV6
    uint32_t wire_length{0}; ///< IP total length in bytes
    int64_t  timestamp_us{0};
    bool     tcp_fin{false};
    bool     tcp_rst{false};
    bool     is_ipv6{false};
};

} // namespace packetpipe
