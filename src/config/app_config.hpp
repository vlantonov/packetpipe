#pragma once
#include <cstdint>
#include <string>

#ifndef PACKETPIPE_VERSION
#define PACKETPIPE_VERSION "0.0.0-dev"
#endif

namespace packetpipe {

/// All runtime configuration derived from CLI arguments.
/// Plain value type; no logic, no dependencies.
struct AppConfig {
    // Ingestion
    std::string pcap_file;
    std::string iface;
    std::string bpf_filter;

    // Flow table
    int flow_timeout_sec{60};
    int max_flows{100'000};
    int stats_interval_sec{30};

    // Kafka
    std::string kafka_brokers{"localhost:9092"};
    std::string kafka_topic{"packetpipe.flows"};
    std::string kafka_conf_file;

    // Schema Registry
    std::string schema_registry_url{"http://localhost:8081"};
    int         sr_buffer_size{1'000};

    // Metrics
    uint16_t metrics_port{9090};

    // Logging
    std::string log_level{"info"};

    // Injected at compile time via -DPACKETPIPE_VERSION
    std::string version{PACKETPIPE_VERSION};
};

} // namespace packetpipe
