#pragma once
#include <array>
#include <cstdint>
#include <functional>

namespace packetpipe {

/// Identifies a unidirectional flow by its 5-tuple.
/// IPv4 addresses are stored in IPv4-mapped-IPv6 form (bytes 0-9 = 0, 10-11 = 0xFF).
struct FlowKey {
    std::array<uint8_t, 16> src_ip{};
    std::array<uint8_t, 16> dst_ip{};
    uint16_t src_port{0};  ///< ICMP: (type << 8) | code
    uint16_t dst_port{0};
    uint8_t  protocol{0};  ///< IPPROTO_* value

    bool operator==(const FlowKey&) const noexcept = default;
};

/// FNV-1a hash over the explicit key fields (no padding dependency).
struct FlowKeyHash {
    size_t operator()(const FlowKey& k) const noexcept {
        constexpr uint64_t FNV_OFFSET = 14695981039346656037ULL;
        constexpr uint64_t FNV_PRIME  = 1099511628211ULL;
        uint64_t h = FNV_OFFSET;
        auto mix = [&](uint8_t b) noexcept { h ^= b; h *= FNV_PRIME; };
        for (auto b : k.src_ip) mix(b);
        for (auto b : k.dst_ip) mix(b);
        mix(static_cast<uint8_t>(k.src_port >> 8));
        mix(static_cast<uint8_t>(k.src_port & 0xFF));
        mix(static_cast<uint8_t>(k.dst_port >> 8));
        mix(static_cast<uint8_t>(k.dst_port & 0xFF));
        mix(k.protocol);
        return static_cast<size_t>(h);
    }
};

} // namespace packetpipe
