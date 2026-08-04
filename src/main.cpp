#include "config/app_config.hpp"
#include "config/config_parser.hpp"
#include "packets/ipacket_source.hpp"
#include "packets/pcap_file_source.hpp"
#include "packets/live_capture_source.hpp"
#include "flowtable/flow_table.hpp"
#include "avro/schema_registry_client.hpp"
#include "avro/avro_serializer.hpp"
#include "kafka/kafka_producer.hpp"
#include "kafka/avro_kafka_sink.hpp"
#include "events/event_queue.hpp"
#include "metrics/metrics_registry.hpp"

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <ctime>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>

namespace packetpipe {

namespace {

// Shared with signal handler – must be signal-handler-safe
std::atomic<bool>  g_shutdown{false};
IPacketSource*     g_source_ptr{nullptr};

// Schema file path: prefer compile-time PACKETPIPE_SCHEMA_DIR, fall back to current dir
std::string schema_path() {
    return std::string(PACKETPIPE_SCHEMA_DIR) + "/FlowEvent.avsc";
}

void setup_logger(const std::string& level) {
    auto logger = spdlog::stdout_color_mt("packetpipe");
    logger->set_pattern("[%Y-%m-%dT%H:%M:%S.%eZ] [%^%-5l%$] [%n] %v");
    if (level == "error")      logger->set_level(spdlog::level::err);
    else if (level == "warn")  logger->set_level(spdlog::level::warn);
    else if (level == "debug") logger->set_level(spdlog::level::debug);
    else                       logger->set_level(spdlog::level::info);
    spdlog::set_default_logger(logger);
}

extern "C" void signal_handler(int /*sig*/) {
    g_shutdown.store(true, std::memory_order_relaxed);
    if (g_source_ptr) g_source_ptr->stop();
}

int64_t now_us() noexcept {
    struct timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1'000'000LL + ts.tv_nsec / 1000LL;
}

} // namespace

} // namespace packetpipe

int main(int argc, char* argv[]) {
    using namespace packetpipe;

    // ── 1. Parse CLI ─────────────────────────────────────────────────────────
    AppConfig cfg;
    try {
        cfg = ConfigParser::parse(argc, argv);
    } catch (const std::invalid_argument& e) {
        std::cerr << "[error] " << e.what() << "\n";
        return 1;
    }

    setup_logger(cfg.log_level);
    spdlog::info("PacketPipe v{} starting", cfg.version);

    try {
        // ── 2. Metrics server ────────────────────────────────────────────────
        MetricsRegistry metrics(cfg.version, cfg.metrics_port);
        spdlog::info("Metrics available at http://0.0.0.0:{}/metrics", cfg.metrics_port);

        // ── 3. Schema Registry ───────────────────────────────────────────────
        SchemaRegistryClient sr_client(cfg.schema_registry_url,
                                       "packetpipe-FlowEvent-value");
        spdlog::info("Schema ID from registry: {}", sr_client.get_schema_id());

        // ── 4. Avro serializer ───────────────────────────────────────────────
        AvroSerializer avro_ser(schema_path(), sr_client);

        // ── 5. Kafka producer ────────────────────────────────────────────────
        KafkaProducer kafka(cfg, metrics);

        // ── 6. Event queue & Avro-Kafka sink ─────────────────────────────────
        auto queue = std::make_shared<DefaultEventQueue>();
        AvroKafkaSink sink(avro_ser, kafka, queue, cfg, metrics);

        // ── 7. Packet source ─────────────────────────────────────────────────
        std::unique_ptr<IPacketSource> source;
        if (!cfg.pcap_file.empty()) {
            source = std::make_unique<PcapFileSource>(cfg.pcap_file, cfg.bpf_filter, &metrics);
        } else {
            source = std::make_unique<LiveCaptureSource>(cfg.iface, cfg.bpf_filter, &metrics);
        }
        g_source_ptr = source.get();
        spdlog::info("Capturing from {}", source->description());

        // ── 8. Flow table ─────────────────────────────────────────────────────
        FlowTable table(cfg, metrics, [&queue](FlowEvent evt) -> bool {
            return queue->push(std::move(evt));
        });

        // ── 9. Signal handlers ────────────────────────────────────────────────
        std::signal(SIGINT,  signal_handler);
        std::signal(SIGTERM, signal_handler);

        // ── 10. Timer thread ──────────────────────────────────────────────────
        std::jthread timer_thread([&table, &cfg](std::stop_token st) {
            const auto interval = std::chrono::milliseconds(
                std::max(1, std::min(cfg.stats_interval_sec, cfg.flow_timeout_sec)) * 500);
            while (!st.stop_requested()) {
                std::this_thread::sleep_for(interval);
                if (!st.stop_requested()) table.timer_sweep(now_us());
            }
        });

        // ── 11. Capture loop (blocks until EOF or signal) ─────────────────────
        source->run([&table](const ParsedPacket& pkt) { table.process(pkt); });

        // ── 12. Shutdown sequence (design §7) ─────────────────────────────────
        spdlog::info("Capture finished – draining active flows");
        table.drain_all(now_us());

        timer_thread.request_stop();
        // timer_thread joined by jthread destructor

        spdlog::info("Flushing Kafka producer (up to 10 s)");
        sink.flush(std::chrono::milliseconds(9000));
        // sink thread joined by AvroKafkaSink destructor

        spdlog::info("PacketPipe shutdown complete");

    } catch (const std::exception& e) {
        spdlog::error("{}", e.what());
        return 1;
    }

    return 0;
}
