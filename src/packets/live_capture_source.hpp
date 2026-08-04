#pragma once
#include "ipacket_source.hpp"
#include <string>

namespace packetpipe {

class IMetricsRegistry;

/// Captures packets from a live network interface (pcap_open_live).
/// Requires CAP_NET_RAW or equivalent privilege.
class LiveCaptureSource final : public IPacketSource {
public:
    /// @throws std::runtime_error if the interface cannot be opened or privilege is insufficient.
    LiveCaptureSource(const std::string& iface,
                      const std::string& bpf_filter = "",
                      IMetricsRegistry* metrics = nullptr);
    ~LiveCaptureSource() override;

    void run(std::function<void(const ParsedPacket&)> callback) override;
    void stop() noexcept override;
    std::string description() const override;

private:
    std::string iface_;
    std::string bpf_filter_;
    IMetricsRegistry* metrics_{nullptr};
    struct pcap* handle_{nullptr};
};

} // namespace packetpipe
