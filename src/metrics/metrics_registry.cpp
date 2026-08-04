#include "metrics_registry.hpp"
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>
#include <stdexcept>
#include <string>

namespace packetpipe {

namespace {

prometheus::Counter& make_counter(prometheus::Registry& reg,
                                  const std::string& name,
                                  const std::string& help,
                                  const prometheus::Labels& labels) {
    return prometheus::BuildCounter()
               .Name(name)
               .Help(help)
               .Register(reg)
               .Add(labels);
}

prometheus::Gauge& make_gauge(prometheus::Registry& reg,
                              const std::string& name,
                              const std::string& help,
                              const prometheus::Labels& labels) {
    return prometheus::BuildGauge()
               .Name(name)
               .Help(help)
               .Register(reg)
               .Add(labels);
}

} // namespace

MetricsRegistry::MetricsRegistry(const std::string& version, uint16_t port)
    : registry_(std::make_shared<prometheus::Registry>()) {

    const prometheus::Labels base{{"version", version}};

    packets_received_          = &make_counter(*registry_,
        "packetpipe_packets_received_total", "Packets ingested", base);
    packets_dropped_           = &make_counter(*registry_,
        "packetpipe_packets_dropped_total",
        "Packets discarded (unrecognised protocol or over flow limit)", base);
    flows_active_              = &make_gauge(*registry_,
        "packetpipe_flows_active", "Currently tracked flows", base);
    flows_created_             = &make_counter(*registry_,
        "packetpipe_flows_created_total", "Total flows opened", base);

    auto& expired_fam = prometheus::BuildCounter()
        .Name("packetpipe_flows_expired_total")
        .Help("Total flows expired")
        .Register(*registry_);
    flows_expired_idle_ = &expired_fam.Add(
        {{"version", version}, {"reason", "idle_timeout"}});
    flows_expired_tcp_  = &expired_fam.Add(
        {{"version", version}, {"reason", "tcp_teardown"}});

    kafka_messages_produced_   = &make_counter(*registry_,
        "packetpipe_kafka_messages_produced_total",
        "Messages handed to librdkafka", base);
    kafka_delivery_errors_     = &make_counter(*registry_,
        "packetpipe_kafka_delivery_errors_total",
        "Failed Kafka deliveries", base);
    avro_serialization_errors_ = &make_counter(*registry_,
        "packetpipe_avro_serialization_errors_total",
        "Avro serialization failures", base);
    schema_registry_retries_   = &make_counter(*registry_,
        "packetpipe_schema_registry_retries_total",
        "Schema Registry retry attempts", base);

    exposer_ = std::make_unique<prometheus::Exposer>(
        "0.0.0.0:" + std::to_string(port));
    exposer_->RegisterCollectable(registry_);
}

MetricsRegistry::~MetricsRegistry() = default;

prometheus::Counter& MetricsRegistry::packets_received()           { return *packets_received_; }
prometheus::Counter& MetricsRegistry::packets_dropped()            { return *packets_dropped_; }
prometheus::Gauge&   MetricsRegistry::flows_active()               { return *flows_active_; }
prometheus::Counter& MetricsRegistry::flows_created()              { return *flows_created_; }

prometheus::Counter& MetricsRegistry::flows_expired(ExpireReason r) {
    return (r == ExpireReason::IdleTimeout) ? *flows_expired_idle_ : *flows_expired_tcp_;
}

prometheus::Counter& MetricsRegistry::kafka_messages_produced()    { return *kafka_messages_produced_; }
prometheus::Counter& MetricsRegistry::kafka_delivery_errors()      { return *kafka_delivery_errors_; }
prometheus::Counter& MetricsRegistry::avro_serialization_errors()  { return *avro_serialization_errors_; }
prometheus::Counter& MetricsRegistry::schema_registry_retries()    { return *schema_registry_retries_; }

} // namespace packetpipe
