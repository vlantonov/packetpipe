#pragma once
#include "ievent_sink.hpp"
#include "avro/avro_serializer.hpp"
#include "kafka_producer.hpp"
#include "events/event_queue.hpp"
#include "metrics/imetrics_registry.hpp"
#include <deque>
#include <memory>
#include <thread>

namespace packetpipe {

struct AppConfig;

/// Runs a background thread that drains EventQueue, serializes events with Avro,
/// and publishes them to Kafka.
class AvroKafkaSink final : public IEventSink {
public:
    AvroKafkaSink(AvroSerializer& serializer,
                  KafkaProducer&  producer,
                  std::shared_ptr<DefaultEventQueue> queue,
                  const AppConfig& cfg,
                  IMetricsRegistry& metrics);
    ~AvroKafkaSink() override;

    bool push(FlowEvent evt) noexcept override;
    void flush(std::chrono::milliseconds timeout) override;

private:
    void sink_thread_func(std::stop_token stop);
    void attempt_produce(const FlowEvent& evt);

    AvroSerializer&                    serializer_;
    KafkaProducer&                     producer_;
    std::shared_ptr<DefaultEventQueue> queue_;
    IMetricsRegistry&                  metrics_;
    int                                sr_buffer_size_{1000};
    std::deque<FlowEvent>              retry_buf_;
    std::jthread                       thread_;
};

} // namespace packetpipe
