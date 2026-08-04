#pragma once
#include "metrics/imetrics_registry.hpp"
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace packetpipe {

struct AppConfig;

struct KafkaInitError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

/// RAII wrapper around an RdKafka::Producer + topic.
/// Delivery callbacks increment Prometheus counters in-place.
class KafkaProducer {
public:
    explicit KafkaProducer(const AppConfig& cfg, IMetricsRegistry& metrics);
    ~KafkaProducer();

    /// Enqueue a message for async delivery.
    void produce(const std::string& key, const std::vector<uint8_t>& payload);

    /// Poll for delivery reports (must be called frequently from the sink thread).
    void poll(int timeout_ms = 0);

    /// Flush outstanding messages; blocks up to timeout_ms.
    void flush(int timeout_ms);

private:
    class DeliveryCb : public RdKafka::DeliveryReportCb {
    public:
        explicit DeliveryCb(IMetricsRegistry& m) : metrics_(m) {}
        void dr_cb(RdKafka::Message& msg) override;
    private:
        IMetricsRegistry& metrics_;
    };

    std::unique_ptr<DeliveryCb>        delivery_cb_;
    std::unique_ptr<RdKafka::Producer> producer_;
    std::string                        topic_name_;
    IMetricsRegistry&                  metrics_;
};

} // namespace packetpipe
