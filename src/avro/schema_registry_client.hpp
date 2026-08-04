#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>

namespace packetpipe {

struct SchemaRegistryUnavailableError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

/// Fetches and caches the schema ID from the Confluent Schema Registry.
class SchemaRegistryClient {
public:
    /// Looks up the schema ID via HTTP GET at startup.
    /// @throws SchemaRegistryUnavailableError if the registry cannot be reached.
    SchemaRegistryClient(const std::string& url, const std::string& subject);

    /// Constructor for testing – bypasses HTTP lookup.
    explicit SchemaRegistryClient(int32_t fixed_schema_id) noexcept
        : schema_id_(fixed_schema_id) {}

    int32_t get_schema_id() const noexcept { return schema_id_; }

private:
    int32_t schema_id_{0};
};

} // namespace packetpipe
