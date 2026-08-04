#include "avro_serializer.hpp"
#include <arpa/inet.h>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/Stream.hh>
#include <fstream>
#include <netinet/in.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <cstring>

namespace packetpipe {

namespace {

std::string ip_array_to_string(const std::array<uint8_t, 16>& ip) {
    // Detect IPv4-mapped (first 10 bytes = 0, bytes 10-11 = 0xFF)
    bool ipv4_mapped =
        (ip[10] == 0xFF && ip[11] == 0xFF &&
         ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0 &&
         ip[4] == 0 && ip[5] == 0 && ip[6] == 0 && ip[7] == 0 &&
         ip[8] == 0 && ip[9] == 0);

    char buf[INET6_ADDRSTRLEN];
    if (ipv4_mapped) {
        struct in_addr addr{};
        std::memcpy(&addr, ip.data() + 12, 4);
        inet_ntop(AF_INET, &addr, buf, sizeof(buf));
    } else {
        struct in6_addr addr{};
        std::memcpy(&addr, ip.data(), 16);
        inet_ntop(AF_INET6, &addr, buf, sizeof(buf));
    }
    return buf;
}

bool is_ipv4_mapped(const std::array<uint8_t, 16>& ip) noexcept {
    return ip[10] == 0xFF && ip[11] == 0xFF &&
           ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0 &&
           ip[4] == 0 && ip[5] == 0 && ip[6] == 0 && ip[7] == 0 &&
           ip[8] == 0 && ip[9] == 0;
}

} // namespace

AvroSerializer::AvroSerializer(const std::string& schema_path,
                               const SchemaRegistryClient& sr_client)
    : schema_id_(sr_client.get_schema_id()) {
    std::ifstream ifs(schema_path);
    if (!ifs) throw std::runtime_error("Cannot open Avro schema: " + schema_path);
    avro::compileJsonSchema(ifs, schema_);

    // Cache field indices for performance
    const avro::NodePtr& root = schema_.root();
    auto idx = [&](const std::string& name, size_t& out) {
        if (!root->nameIndex(name, out)) {
            throw std::runtime_error("Schema missing field: " + name);
        }
    };
    idx("schema_version",    idx_schema_version_);
    idx("event_type",        idx_event_type_);
    idx("flow_id",           idx_flow_id_);
    idx("src_ip",            idx_src_ip_);
    idx("dst_ip",            idx_dst_ip_);
    idx("src_port",          idx_src_port_);
    idx("dst_port",          idx_dst_port_);
    idx("protocol",          idx_protocol_);
    idx("start_timestamp_us", idx_start_ts_);
    idx("event_timestamp_us", idx_event_ts_);
    idx("packet_count",      idx_packet_count_);
    idx("byte_count",        idx_byte_count_);
    idx("expire_reason",     idx_expire_reason_);
}

std::string AvroSerializer::flow_id(const FlowKey& key) {
    const std::string src_str = ip_array_to_string(key.src_ip);
    const std::string dst_str = ip_array_to_string(key.dst_ip);

    const bool src_v6 = !is_ipv4_mapped(key.src_ip);
    const bool dst_v6 = !is_ipv4_mapped(key.dst_ip);

    const std::string src_fmt = src_v6 ? "[" + src_str + "]" : src_str;
    const std::string dst_fmt = dst_v6 ? "[" + dst_str + "]" : dst_str;

    return src_fmt + ":" + std::to_string(key.src_port) + "-" +
           dst_fmt + ":" + std::to_string(key.dst_port) + "/" +
           std::to_string(key.protocol);
}

std::vector<uint8_t> AvroSerializer::serialize(const FlowEvent& evt) const {
    try {
        avro::GenericDatum datum(schema_);
        auto& rec = datum.value<avro::GenericRecord>();

        const std::string fid = flow_id(evt.state.key);

        rec.fieldAt(idx_schema_version_).value<std::string>()  = "1.0";
        rec.fieldAt(idx_flow_id_).value<std::string>()         = fid;
        rec.fieldAt(idx_src_ip_).value<std::string>()          = ip_array_to_string(evt.state.key.src_ip);
        rec.fieldAt(idx_dst_ip_).value<std::string>()          = ip_array_to_string(evt.state.key.dst_ip);
        rec.fieldAt(idx_src_port_).value<int32_t>()            = evt.state.key.src_port;
        rec.fieldAt(idx_dst_port_).value<int32_t>()            = evt.state.key.dst_port;
        rec.fieldAt(idx_protocol_).value<int32_t>()            = evt.state.key.protocol;
        rec.fieldAt(idx_start_ts_).value<int64_t>()            = evt.state.start_us;
        rec.fieldAt(idx_event_ts_).value<int64_t>()            = evt.event_timestamp_us;
        rec.fieldAt(idx_packet_count_).value<int64_t>()        = static_cast<int64_t>(evt.state.packet_count);
        rec.fieldAt(idx_byte_count_).value<int64_t>()          = static_cast<int64_t>(evt.state.byte_count);

        // event_type enum
        {
            std::string sym;
            switch (evt.type) {
                case FlowEventType::Start: sym = "FLOW_START"; break;
                case FlowEventType::End:   sym = "FLOW_END";   break;
                case FlowEventType::Stats: sym = "FLOW_STATS"; break;
            }
            rec.fieldAt(idx_event_type_).value<avro::GenericEnum>().set(sym);
        }

        // expire_reason union ["null", "string"]
        {
            auto& u = rec.fieldAt(idx_expire_reason_).value<avro::GenericUnion>();
            if (evt.type == FlowEventType::End) {
                u.selectBranch(1); // string
                const std::string reason_str =
                    (evt.reason == ExpireReason::IdleTimeout) ? "idle_timeout" : "tcp_teardown";
                u.datum().value<std::string>() = reason_str;
            } else {
                u.selectBranch(0); // null (default)
            }
        }

        // Encode to Avro binary
        std::ostringstream oss;
        auto out = avro::ostreamOutputStream(oss);
        auto enc = avro::binaryEncoder();
        enc->init(*out);
        avro::encode(*enc, datum);
        enc->flush();
        out->flush();

        const std::string payload = oss.str();

        // Build Confluent wire format: 0x00 + 4-byte BE schema_id + avro binary
        std::vector<uint8_t> result;
        result.reserve(5 + payload.size());
        result.push_back(0x00);
        const uint32_t id_be = htonl(static_cast<uint32_t>(schema_id_));
        const uint8_t* id_bytes = reinterpret_cast<const uint8_t*>(&id_be);
        result.push_back(id_bytes[0]);
        result.push_back(id_bytes[1]);
        result.push_back(id_bytes[2]);
        result.push_back(id_bytes[3]);
        result.insert(result.end(), payload.begin(), payload.end());
        return result;

    } catch (const std::exception& ex) {
        throw AvroSerializationError(std::string("Avro serialization failed: ") + ex.what());
    }
}

} // namespace packetpipe
