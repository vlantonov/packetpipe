#pragma once
#include "schema_registry_client.hpp"
#include "events/flow_event.hpp"
#include <avro/Compiler.hh>
#include <avro/ValidSchema.hh>
#include <stdexcept>
#include <string>
#include <vector>

namespace packetpipe {

struct AvroSerializationError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

/// Serializes FlowEvent values to the Confluent wire format:
///   [0x00][4-byte big-endian schema_id][avro binary payload]
class AvroSerializer {
public:
    /// @param schema_path  Path to FlowEvent.avsc.
    /// @param sr_client    Provides the schema ID for the wire-format header.
    AvroSerializer(const std::string& schema_path,
                   const SchemaRegistryClient& sr_client);

    /// @throws AvroSerializationError on any encoding failure.
    std::vector<uint8_t> serialize(const FlowEvent& evt) const;

    /// Canonical flow_id / Kafka message key from a FlowKey.
    static std::string flow_id(const FlowKey& key);

private:
    avro::ValidSchema schema_;
    int32_t           schema_id_{0};

    // Cached field indices – looked up once from the schema
    size_t idx_schema_version_{0};
    size_t idx_event_type_{0};
    size_t idx_flow_id_{0};
    size_t idx_src_ip_{0};
    size_t idx_dst_ip_{0};
    size_t idx_src_port_{0};
    size_t idx_dst_port_{0};
    size_t idx_protocol_{0};
    size_t idx_start_ts_{0};
    size_t idx_event_ts_{0};
    size_t idx_packet_count_{0};
    size_t idx_byte_count_{0};
    size_t idx_expire_reason_{0};
};

} // namespace packetpipe
