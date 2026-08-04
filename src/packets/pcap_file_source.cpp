#include "pcap_file_source.hpp"
#include "packet_decoder.hpp"
#include <pcap/pcap.h>
#include <stdexcept>
#include <cstring>

namespace packetpipe {

namespace {

struct CallbackCtx {
    std::function<void(const ParsedPacket&)>* callback;
};

void pcap_handler(u_char* user, const struct pcap_pkthdr* hdr, const u_char* bytes) {
    auto* ctx = reinterpret_cast<CallbackCtx*>(user);
    auto pkt = PacketDecoder::decode(bytes, hdr->caplen, hdr->ts);
    if (pkt) (*ctx->callback)(*pkt);
}

} // namespace

PcapFileSource::PcapFileSource(const std::string& path, const std::string& bpf_filter)
    : path_(path), bpf_filter_(bpf_filter) {}

PcapFileSource::~PcapFileSource() {
    if (handle_) { pcap_close(handle_); handle_ = nullptr; }
}

void PcapFileSource::run(std::function<void(const ParsedPacket&)> callback) {
    char errbuf[PCAP_ERRBUF_SIZE];
    handle_ = pcap_open_offline(path_.c_str(), errbuf);
    if (!handle_) throw std::runtime_error("Cannot open pcap file: " + std::string(errbuf));

    if (!bpf_filter_.empty()) {
        struct bpf_program fp{};
        if (pcap_compile(handle_, &fp, bpf_filter_.c_str(), 1, PCAP_NETMASK_UNKNOWN) != 0) {
            const std::string err = pcap_geterr(handle_);
            pcap_close(handle_); handle_ = nullptr;
            throw std::runtime_error("BPF filter compile error: " + err);
        }
        if (pcap_setfilter(handle_, &fp) != 0) {
            const std::string err = pcap_geterr(handle_);
            pcap_freecode(&fp);
            pcap_close(handle_); handle_ = nullptr;
            throw std::runtime_error("BPF setfilter error: " + err);
        }
        pcap_freecode(&fp);
    }

    CallbackCtx ctx{&callback};
    pcap_loop(handle_, 0, pcap_handler, reinterpret_cast<u_char*>(&ctx));
    pcap_close(handle_);
    handle_ = nullptr;
}

void PcapFileSource::stop() noexcept {
    if (handle_) pcap_breakloop(handle_);
}

std::string PcapFileSource::description() const {
    return "pcap-file:" + path_;
}

} // namespace packetpipe
