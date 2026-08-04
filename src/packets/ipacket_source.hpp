#pragma once
#include "parsed_packet.hpp"
#include <functional>
#include <string>

namespace packetpipe {

/// Abstract packet source (pcap file or live interface).
class IPacketSource {
public:
    virtual ~IPacketSource() = default;

    /// Blocks until end-of-file (pcap) or stop() is called (live capture).
    /// Calls callback synchronously for every successfully decoded packet.
    /// Must be called from the capture thread only.
    virtual void run(std::function<void(const ParsedPacket&)> callback) = 0;

    /// Thread-safe. Signals run() to return on the next packet boundary.
    virtual void stop() noexcept = 0;

    /// Human-readable description for log output.
    virtual std::string description() const = 0;
};

} // namespace packetpipe
