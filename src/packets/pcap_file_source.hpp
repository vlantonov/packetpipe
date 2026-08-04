#pragma once
#include "ipacket_source.hpp"
#include <string>

namespace packetpipe {

/// Reads all packets from an offline pcap file (pcap_open_offline).
class PcapFileSource final : public IPacketSource {
public:
    explicit PcapFileSource(const std::string& path, const std::string& bpf_filter = "");
    ~PcapFileSource() override;

    void run(std::function<void(const ParsedPacket&)> callback) override;
    void stop() noexcept override;
    std::string description() const override;

private:
    std::string path_;
    std::string bpf_filter_;
    struct pcap* handle_{nullptr};
};

} // namespace packetpipe
