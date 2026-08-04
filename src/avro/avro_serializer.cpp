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
        constexpr size_t kSchemaVersionIdx = 0;
        constexpr size_t kEventTypeIdx = 1;
        constexpr size_t kFlowIdIdx = 2;
        constexpr size_t kSrcIpIdx = 3;
        constexpr size_t kDstIpIdx = 4;
        constexpr size_t kSrcPortIdx = 5;
        constexpr size_t kDstPortIdx = 6;
        constexpr size_t kProtocolIdx = 7;
        constexpr size_t kStartTsIdx = 8;
        constexpr size_t kEventTsIdx = 9;
        constexpr size_t kPacketCountIdx = 10;
        constexpr size_t kByteCountIdx = 11;
        constexpr size_t kExpireReasonIdx = 12;

        avro::GenericDatum datum(schema_);
        auto& rec = datum.value<avro::GenericRecord>();

        if (rec.fieldCount() <= kExpireReasonIdx) {
            throw AvroSerializationError("Avro schema field layout mismatch for FlowEvent");
        }

        const std::string fid = flow_id(evt.state.key);

        rec.fieldAt(kSchemaVersionIdx).value<std::string>()  = "1.0";
        rec.fieldAt(kFlowIdIdx).value<std::string>()         = fid;
        rec.fieldAt(kSrcIpIdx).value<std::string>()          = ip_array_to_string(evt.state.key.src_ip);
        rec.fieldAt(kDstIpIdx).value<std::string>()          = ip_array_to_string(evt.state.key.dst_ip);
        rec.fieldAt(kSrcPortIdx).value<int32_t>()            = evt.state.key.src_port;
        rec.fieldAt(kDstPortIdx).value<int32_t>()            = evt.state.key.dst_port;
        rec.fieldAt(kProtocolIdx).value<int32_t>()           = evt.state.key.protocol;
        rec.fieldAt(kStartTsIdx).value<int64_t>()            = evt.state.start_us;
        rec.fieldAt(kEventTsIdx).value<int64_t>()            = evt.event_timestamp_us;
        rec.fieldAt(kPacketCountIdx).value<int64_t>()        = static_cast<int64_t>(evt.state.packet_count);
        rec.fieldAt(kByteCountIdx).value<int64_t>()          = static_cast<int64_t>(evt.state.byte_count);

        // event_type enum
        {
            std::string sym;
            switch (evt.type) {
                case FlowEventType::Start: sym = "FLOW_START"; break;
                case FlowEventType::End:   sym = "FLOW_END";   break;
                case FlowEventType::Stats: sym = "FLOW_STATS"; break;
            }
            rec.fieldAt(kEventTypeIdx).value<avro::GenericEnum>().set(sym);
        }

        // expire_reason union ["null", "string"]
        {
            auto& expire_reason = rec.fieldAt(kExpireReasonIdx);
            if (evt.type == FlowEventType::End) {
                expire_reason.selectBranch(1); // string
                const std::string reason_str =
                    (evt.reason == ExpireReason::IdleTimeout) ? "idle_timeout" : "tcp_teardown";
                expire_reason.value<std::string>() = reason_str;
            } else {
                expire_reason.selectBranch(0); // null (default)
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
