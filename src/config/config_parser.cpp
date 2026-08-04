#include "config_parser.hpp"
#include <algorithm>
#include <stdexcept>
#include <string>
#include <string_view>

namespace packetpipe {

namespace {

const char* kUsage =
    "Usage: packetpipe --pcap <file> | --iface <name> [options]\n"
    "\n"
    "Ingestion (exactly one required):\n"
    "  --pcap <file>            Read packets from a pcap file\n"
    "  --iface <name>           Capture from a live network interface\n"
    "\n"
    "Options:\n"
    "  --filter <bpf>           BPF filter expression (default: none)\n"
    "  --flow-timeout <sec>     Idle flow expiry timeout (default: 60)\n"
    "  --max-flows <N>          Maximum simultaneous flows (default: 100000)\n"
    "  --stats-interval <sec>   FlowStats heartbeat interval (default: 30)\n"
    "  --kafka-brokers <list>   Kafka broker list (default: localhost:9092)\n"
    "  --kafka-topic <name>     Kafka topic (default: packetpipe.flows)\n"
    "  --kafka-conf <file>      librdkafka config file (optional)\n"
    "  --schema-registry-url <url>  Schema Registry URL (default: http://localhost:8081)\n"
    "  --sr-buffer <N>          SR retry buffer size (default: 1000)\n"
    "  --metrics-port <port>    Prometheus metrics port (default: 9090)\n"
    "  --log-level <level>      Log level: error|warn|info|debug (default: info)\n"
    "  --help                   Show this help\n";

[[noreturn]] void die(const std::string& msg) {
    throw std::invalid_argument(msg + "\n\n" + kUsage);
}

std::string next_arg(int argc, char* argv[], int& i, std::string_view flag) {
    ++i;
    if (i >= argc) die(std::string("Flag ") + std::string(flag) + " requires a value");
    return argv[i];
}

int parse_positive_int(const std::string& s, std::string_view flag) {
    try {
        int v = std::stoi(s);
        if (v <= 0) die(std::string(flag) + " must be a positive integer");
        return v;
    } catch (const std::exception&) {
        die(std::string(flag) + " requires an integer value, got: " + s);
    }
}

} // anonymous namespace

AppConfig ConfigParser::parse(int argc, char* argv[]) {
    AppConfig cfg;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            throw std::invalid_argument(kUsage);
        } else if (arg == "--pcap") {
            cfg.pcap_file = next_arg(argc, argv, i, arg);
        } else if (arg == "--iface") {
            cfg.iface = next_arg(argc, argv, i, arg);
        } else if (arg == "--filter") {
            cfg.bpf_filter = next_arg(argc, argv, i, arg);
        } else if (arg == "--flow-timeout") {
            cfg.flow_timeout_sec = parse_positive_int(next_arg(argc, argv, i, arg), arg);
        } else if (arg == "--max-flows") {
            cfg.max_flows = parse_positive_int(next_arg(argc, argv, i, arg), arg);
        } else if (arg == "--stats-interval") {
            cfg.stats_interval_sec = parse_positive_int(next_arg(argc, argv, i, arg), arg);
        } else if (arg == "--kafka-brokers") {
            cfg.kafka_brokers = next_arg(argc, argv, i, arg);
        } else if (arg == "--kafka-topic") {
            cfg.kafka_topic = next_arg(argc, argv, i, arg);
        } else if (arg == "--kafka-conf") {
            cfg.kafka_conf_file = next_arg(argc, argv, i, arg);
        } else if (arg == "--schema-registry-url") {
            cfg.schema_registry_url = next_arg(argc, argv, i, arg);
        } else if (arg == "--sr-buffer") {
            cfg.sr_buffer_size = parse_positive_int(next_arg(argc, argv, i, arg), arg);
        } else if (arg == "--metrics-port") {
            int p = parse_positive_int(next_arg(argc, argv, i, arg), arg);
            if (p > 65535) die("--metrics-port must be in range 1-65535");
            cfg.metrics_port = static_cast<uint16_t>(p);
        } else if (arg == "--log-level") {
            cfg.log_level = next_arg(argc, argv, i, arg);
            static constexpr std::string_view kLevels[] = {"error", "warn", "info", "debug"};
            bool valid = std::any_of(std::begin(kLevels), std::end(kLevels),
                                     [&](auto l) { return l == cfg.log_level; });
            if (!valid) die("--log-level must be one of: error warn info debug");
        } else {
            die("Unknown argument: " + std::string(arg));
        }
    }

    // Exactly one of --pcap or --iface is required (FR-ING-3)
    if (cfg.pcap_file.empty() && cfg.iface.empty()) {
        die("Either --pcap <file> or --iface <name> is required");
    }
    if (!cfg.pcap_file.empty() && !cfg.iface.empty()) {
        die("--pcap and --iface are mutually exclusive");
    }

    return cfg;
}

} // namespace packetpipe
