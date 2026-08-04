#include "live_capture_source.hpp"
#include "packet_decoder.hpp"
#include "metrics/imetrics_registry.hpp"
#include <pcap/pcap.h>
#include <sys/capability.h>
#include <stdexcept>
#include <cstring>

namespace packetpipe {

namespace {

/// Returns true if the process has CAP_NET_RAW (NFR-PORT-3).
bool has_cap_net_raw() noexcept {
#ifdef __linux__
    cap_t caps = cap_get_proc();
    if (!caps) return false;
    cap_flag_value_t val = CAP_CLEAR;
    cap_get_flag(caps, CAP_NET_RAW, CAP_EFFECTIVE, &val);
    cap_free(caps);
    return val == CAP_SET;
#else
    return true; // Best-effort on non-Linux
#endif
}

struct CallbackCtx {
    std::function<void(const ParsedPacket&)>* callback;
    IMetricsRegistry* metrics;
};

void pcap_handler(u_char* user, const struct pcap_pkthdr* hdr, const u_char* bytes) {
    auto* ctx = reinterpret_cast<CallbackCtx*>(user);
    auto pkt = PacketDecoder::decode(bytes, hdr->caplen, hdr->ts);
    if (pkt) {
        (*ctx->callback)(*pkt);
    } else if (ctx->metrics) {
        ctx->metrics->packets_dropped().Increment();
    }
}

} // namespace

LiveCaptureSource::LiveCaptureSource(const std::string& iface,
                                     const std::string& bpf_filter,
                                     IMetricsRegistry* metrics)
    : iface_(iface), bpf_filter_(bpf_filter), metrics_(metrics) {
    if (!has_cap_net_raw()) {
        throw std::runtime_error(
            "Live capture requires CAP_NET_RAW. Run as root or grant the capability:\n"
            "  sudo setcap cap_net_raw+eip ./packetpipe");
    }
}

LiveCaptureSource::~LiveCaptureSource() {
    if (handle_) { pcap_close(handle_); handle_ = nullptr; }
}

void LiveCaptureSource::run(std::function<void(const ParsedPacket&)> callback) {
    char errbuf[PCAP_ERRBUF_SIZE];
    handle_ = pcap_open_live(iface_.c_str(), 65535, /*promisc=*/1, /*to_ms=*/100, errbuf);
    if (!handle_) throw std::runtime_error("Cannot open interface " + iface_ + ": " + errbuf);

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

    // Set non-blocking so pcap_dispatch returns on timeout, enabling stop() to work
    if (pcap_setnonblock(handle_, 1, errbuf) != 0) {
        throw std::runtime_error("pcap_setnonblock: " + std::string(errbuf));
    }

    CallbackCtx ctx{&callback, metrics_};
    // Use pcap_dispatch in a loop so pcap_breakloop can interrupt us
    while (true) {
        int r = pcap_dispatch(handle_, 100, pcap_handler, reinterpret_cast<u_char*>(&ctx));
        if (r == PCAP_ERROR_BREAK) break;
        if (r == PCAP_ERROR) {
            throw std::runtime_error("pcap_dispatch error: " + std::string(pcap_geterr(handle_)));
        }
    }

    pcap_close(handle_);
    handle_ = nullptr;
}

void LiveCaptureSource::stop() noexcept {
    if (handle_) pcap_breakloop(handle_);
}

std::string LiveCaptureSource::description() const {
    return "live-iface:" + iface_;
}

} // namespace packetpipe
