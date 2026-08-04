#include "avro_kafka_sink.hpp"
#include "config/app_config.hpp"
#include "avro/avro_serializer.hpp"
#include <spdlog/spdlog.h>

namespace packetpipe {

AvroKafkaSink::AvroKafkaSink(AvroSerializer& serializer,
                              KafkaProducer&  producer,
                              std::shared_ptr<DefaultEventQueue> queue,
                              const AppConfig& cfg,
                              IMetricsRegistry& metrics)
    : serializer_(serializer)
    , producer_(producer)
    , queue_(std::move(queue))
    , metrics_(metrics)
    , sr_buffer_size_(cfg.sr_buffer_size)
    , thread_([this](std::stop_token st) { sink_thread_func(std::move(st)); }) {}

AvroKafkaSink::~AvroKafkaSink() {
    thread_.request_stop();
    // thread_ destructor joins
}

bool AvroKafkaSink::push(FlowEvent evt) noexcept {
    return queue_->push(std::move(evt));
}

void AvroKafkaSink::flush(std::chrono::milliseconds timeout) {
    producer_.flush(static_cast<int>(timeout.count()));
}

bool AvroKafkaSink::attempt_produce(const FlowEvent& evt) {
    try {
        const auto payload = serializer_.serialize(evt);
        const std::string key = AvroSerializer::flow_id(evt.state.key);
        return producer_.produce(key, payload);
    } catch (const AvroSerializationError& ex) {
        metrics_.avro_serialization_errors().Increment();
        spdlog::warn("[avro-kafka] serialization error: {}", ex.what());
        return true; // not retriable
    }
}

void AvroKafkaSink::sink_thread_func(std::stop_token stop) {
    spdlog::info("[avro-kafka] sink thread started");

    while (!stop.stop_requested() || !queue_->empty()) {
        // Drain retry buffer first (FR-SER-4)
        while (!retry_buf_.empty()) {
            const FlowEvent& evt = retry_buf_.front();
            try {
                const auto payload = serializer_.serialize(evt);
                const std::string key = AvroSerializer::flow_id(evt.state.key);
                if (!producer_.produce(key, payload)) {
                    break; // still failing – leave in buffer and retry next iteration
                }
                metrics_.schema_registry_retries().Increment();
                retry_buf_.pop_front();
            } catch (const AvroSerializationError&) {
                metrics_.avro_serialization_errors().Increment();
                retry_buf_.pop_front(); // discard un-serializable events
            }
        }

        // Consume from queue
        auto maybe_evt = queue_->pop_wait(std::chrono::milliseconds(10));
        if (maybe_evt) {
            if (!attempt_produce(*maybe_evt)) {
                if (static_cast<int>(retry_buf_.size()) < sr_buffer_size_) {
                    retry_buf_.push_back(std::move(*maybe_evt));
                } else {
                    metrics_.kafka_delivery_errors().Increment();
                    spdlog::warn("[avro-kafka] retry buffer full – dropping event");
                }
            }
        }

        producer_.poll(0);
    }

    spdlog::info("[avro-kafka] draining Kafka producer queue (up to 9 s)");
    producer_.flush(9000);
    spdlog::info("[avro-kafka] sink thread stopped");
}

} // namespace packetpipe
