#include "schema_registry_client.hpp"
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>

namespace packetpipe {

namespace {

/// Parses "http://host:port" into host and port components.
std::pair<std::string, int> parse_url(const std::string& url) {
    const auto scheme_end = url.find("://");
    if (scheme_end == std::string::npos) {
        throw SchemaRegistryUnavailableError("Invalid Schema Registry URL: " + url);
    }
    const std::string host_port = url.substr(scheme_end + 3);
    const auto colon = host_port.rfind(':');
    if (colon == std::string::npos) {
        return {host_port, 80};
    }
    return {host_port.substr(0, colon), std::stoi(host_port.substr(colon + 1))};
}

} // namespace

SchemaRegistryClient::SchemaRegistryClient(const std::string& url,
                                           const std::string& subject) {
    const auto [host, port] = parse_url(url);
    httplib::Client cli(host, port);
    cli.set_connection_timeout(5);
    cli.set_read_timeout(5);

    const std::string path = "/subjects/" + subject + "/versions/latest";
    auto res = cli.Get(path);
    if (!res) {
        throw SchemaRegistryUnavailableError(
            "Cannot reach Schema Registry at " + url + ": " + httplib::to_string(res.error()));
    }
    if (res->status != 200) {
        throw SchemaRegistryUnavailableError(
            "Schema Registry returned HTTP " + std::to_string(res->status) +
            " for subject '" + subject + "'. Register the schema first.");
    }

    const auto j = nlohmann::json::parse(res->body);
    if (!j.contains("id")) {
        throw SchemaRegistryUnavailableError(
            "Schema Registry response missing 'id' field: " + res->body);
    }
    schema_id_ = j["id"].get<int32_t>();
}

} // namespace packetpipe
