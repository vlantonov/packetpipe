#pragma once
#include "imetrics_registry.hpp"
#include <prometheus/registry.h>
#include <memory>
#include <string>

namespace prometheus { class Exposer; }

namespace packetpipe {

/// Concrete implementation; creates all metric families and starts the HTTP exposer.
class MetricsRegistry final : public IMetricsRegistry {
public:
    /// @param version  SemVer string attached as a label to every metric.
    /// @param port     Port on which /metrics is served.
    MetricsRegistry(const std::string& version, uint16_t port);
    ~MetricsRegistry() override;

    // IMetricsRegistry
    prometheus::Counter& packets_received()            override;
    prometheus::Counter& packets_dropped()             override;
    prometheus::Gauge&   flows_active()                override;
    prometheus::Counter& flows_created()               override;
    prometheus::Counter& flows_expired(ExpireReason r) override;
    prometheus::Counter& kafka_messages_produced()     override;
    prometheus::Counter& kafka_delivery_errors()       override;
    prometheus::Counter& avro_serialization_errors()   override;
    prometheus::Counter& schema_registry_retries()     override;

private:
    std::shared_ptr<prometheus::Registry> registry_;
    std::unique_ptr<prometheus::Exposer>  exposer_;

    prometheus::Counter* packets_received_{nullptr};
    prometheus::Counter* packets_dropped_{nullptr};
    prometheus::Gauge*   flows_active_{nullptr};
    prometheus::Counter* flows_created_{nullptr};
    prometheus::Counter* flows_expired_idle_{nullptr};
    prometheus::Counter* flows_expired_tcp_{nullptr};
    prometheus::Counter* kafka_messages_produced_{nullptr};
    prometheus::Counter* kafka_delivery_errors_{nullptr};
    prometheus::Counter* avro_serialization_errors_{nullptr};
    prometheus::Counter* schema_registry_retries_{nullptr};
};

} // namespace packetpipe
