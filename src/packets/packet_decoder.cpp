#include "packet_decoder.hpp"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/ip6.h>
#include <netinet/ip_icmp.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <sys/time.h>
#include <cstring>

namespace packetpipe {

namespace {

// Ethernet frame constants
constexpr uint32_t kEthHdrLen   = 14;
constexpr uint16_t kEtherIPv4   = 0x0800;
constexpr uint16_t kEtherIPv6   = 0x86DD;
constexpr uint16_t kEtherVLAN   = 0x8100;

void set_ipv4_mapped(std::array<uint8_t, 16>& out, const void* addr4) noexcept {
    out.fill(0);
    out[10] = 0xFF;
    out[11] = 0xFF;
    std::memcpy(out.data() + 12, addr4, 4);
}

} // namespace

std::optional<ParsedPacket> PacketDecoder::decode(const uint8_t* data,
                                                   uint32_t len,
                                                   const timeval& ts) noexcept {
    if (!data || len < kEthHdrLen) return std::nullopt;

    // Ethernet header
    uint16_t ethertype;
    uint32_t offset = kEthHdrLen - 2;
    std::memcpy(&ethertype, data + offset, 2);
    ethertype = ntohs(ethertype);
    offset += 2;

    // Unwrap single VLAN tag
    if (ethertype == kEtherVLAN) {
        if (len < offset + 4) return std::nullopt;
        offset += 2; // skip 802.1Q TCI
        std::memcpy(&ethertype, data + offset, 2);
        ethertype = ntohs(ethertype);
        offset += 2;
    }

    ParsedPacket pkt{};
    pkt.timestamp_us = static_cast<int64_t>(ts.tv_sec) * 1'000'000LL + ts.tv_usec;

    if (ethertype == kEtherIPv4) {
        if (len < offset + sizeof(struct ip)) return std::nullopt;
        const auto* ip4 = reinterpret_cast<const struct ip*>(data + offset);
        if (ip4->ip_v != 4) return std::nullopt;

        const uint32_t ip_hdr_len = static_cast<uint32_t>(ip4->ip_hl) * 4u;
        if (ip_hdr_len < 20 || len < offset + ip_hdr_len) return std::nullopt;

        set_ipv4_mapped(pkt.src_ip, &ip4->ip_src);
        set_ipv4_mapped(pkt.dst_ip, &ip4->ip_dst);
        pkt.protocol    = ip4->ip_p;
        pkt.wire_length = ntohs(ip4->ip_len);
        pkt.is_ipv6     = false;
        offset         += ip_hdr_len;

    } else if (ethertype == kEtherIPv6) {
        if (len < offset + sizeof(struct ip6_hdr)) return std::nullopt;
        const auto* ip6 = reinterpret_cast<const struct ip6_hdr*>(data + offset);
        if ((ip6->ip6_vfc >> 4) != 6) return std::nullopt;

        std::memcpy(pkt.src_ip.data(), &ip6->ip6_src, 16);
        std::memcpy(pkt.dst_ip.data(), &ip6->ip6_dst, 16);
        pkt.protocol    = ip6->ip6_nxt;
        pkt.wire_length = ntohs(ip6->ip6_plen) + 40u;
        pkt.is_ipv6     = true;
        offset         += sizeof(struct ip6_hdr);

    } else {
        return std::nullopt; // non-IP Ethernet type
    }

    // Transport layer
    switch (pkt.protocol) {
        case IPPROTO_TCP: {
            if (len < offset + sizeof(struct tcphdr)) return std::nullopt;
            const auto* tcp = reinterpret_cast<const struct tcphdr*>(data + offset);
            pkt.src_port = ntohs(tcp->th_sport);
            pkt.dst_port = ntohs(tcp->th_dport);
            pkt.tcp_fin  = (tcp->th_flags & TH_FIN) != 0;
            pkt.tcp_rst  = (tcp->th_flags & TH_RST) != 0;
            break;
        }
        case IPPROTO_UDP: {
            if (len < offset + sizeof(struct udphdr)) return std::nullopt;
            const auto* udp = reinterpret_cast<const struct udphdr*>(data + offset);
            pkt.src_port = ntohs(udp->uh_sport);
            pkt.dst_port = ntohs(udp->uh_dport);
            break;
        }
        case IPPROTO_ICMP: {
            if (len < offset + sizeof(struct icmphdr)) return std::nullopt;
            const auto* icmp = reinterpret_cast<const struct icmphdr*>(data + offset);
            // Encode ICMP type in high byte and code in low byte (FR-FLOW-1)
            pkt.src_port = static_cast<uint16_t>(
                (static_cast<uint16_t>(icmp->type) << 8) | icmp->code);
            break;
        }
        case IPPROTO_ICMPV6: {
            if (len < offset + 4) return std::nullopt;
            pkt.src_port = static_cast<uint16_t>(
                (static_cast<uint16_t>(data[offset]) << 8) | data[offset + 1]);
            break;
        }
        default:
            return std::nullopt; // unrecognised upper-layer protocol
    }

    return pkt;
}

} // namespace packetpipe
