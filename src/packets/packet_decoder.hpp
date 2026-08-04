#pragma once
#include "parsed_packet.hpp"
#include <cstdint>
#include <optional>
#include <string>

struct timeval;

namespace packetpipe {

/// Stateless L2/L3/L4 packet decoder.
/// Input: raw bytes from libpcap (DLT_EN10MB assumed).
/// Output: ParsedPacket on success, nullopt on any decode error.
class PacketDecoder {
public:
    /// @param data       Pointer to the start of the Ethernet frame.
    /// @param len        Captured length (may be less than wire_length).
    /// @param ts         libpcap timestamp.
    static std::optional<ParsedPacket> decode(const uint8_t* data,
                                               uint32_t len,
                                               const timeval& ts) noexcept;
};

} // namespace packetpipe
