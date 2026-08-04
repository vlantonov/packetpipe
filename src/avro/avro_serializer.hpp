#pragma once
#include "schema_registry_client.hpp"
#include "events/flow_event.hpp"
#include <fmt/format.h>
#include <avro/Compiler.hh>
#include <avro/Specific.hh>
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
};

} // namespace packetpipe
