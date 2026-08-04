#include "kafka_producer.hpp"
#include "config/app_config.hpp"
#include <fstream>
#include <spdlog/spdlog.h>
#include <sstream>

namespace packetpipe {

KafkaProducer::KafkaProducer(const AppConfig& cfg, IMetricsRegistry& metrics)
    : metrics_(metrics)
    , topic_name_(cfg.kafka_topic)
    , delivery_cb_(std::make_unique<DeliveryCb>(metrics)) {

    std::string errstr;
    auto conf = std::unique_ptr<RdKafka::Conf>(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

    // Layer 1: hard-coded defaults
    auto set = [&](const std::string& key, const std::string& val) {
        if (conf->set(key, val, errstr) != RdKafka::Conf::CONF_OK) {
            throw KafkaInitError("rdkafka conf set " + key + ": " + errstr);
        }
    };
    set("compression.codec",         "snappy");
    set("batch.num.messages",        "1000");
    set("linger.ms",                 "5");
    set("enable.idempotence",        "false");
    set("message.send.max.retries",  "3");

    // Layer 2: optional librdkafka config file
    if (!cfg.kafka_conf_file.empty()) {
        std::ifstream f(cfg.kafka_conf_file);
        if (!f) throw KafkaInitError("Cannot open kafka conf file: " + cfg.kafka_conf_file);
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty() || line[0] == '#') continue;
            const auto eq = line.find('=');
            if (eq == std::string::npos) continue;
            set(line.substr(0, eq), line.substr(eq + 1));
        }
    }

    // Layer 3: explicit overrides
    set("bootstrap.servers", cfg.kafka_brokers);

    if (conf->set("dr_cb", delivery_cb_.get(), errstr) != RdKafka::Conf::CONF_OK) {
        throw KafkaInitError("rdkafka set dr_cb: " + errstr);
    }

    producer_.reset(RdKafka::Producer::create(conf.release(), errstr));
    if (!producer_) throw KafkaInitError("Failed to create Kafka producer: " + errstr);
}

KafkaProducer::~KafkaProducer() = default;

bool KafkaProducer::produce(const std::string& key, const std::vector<uint8_t>& payload) {
    RdKafka::ErrorCode err = producer_->produce(
        topic_name_,
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<uint8_t*>(payload.data()), payload.size(),
        key.data(), key.size(),
        0, nullptr, nullptr);

    if (err != RdKafka::ERR_NO_ERROR) {
        spdlog::warn("[kafka] produce error: {}", RdKafka::err2str(err));
        metrics_.kafka_delivery_errors().Increment();
        return false;
    }
    metrics_.kafka_messages_produced().Increment();
    return true;
}

void KafkaProducer::poll(int timeout_ms) {
    producer_->poll(timeout_ms);
}

void KafkaProducer::flush(int timeout_ms) {
    const RdKafka::ErrorCode err = producer_->flush(timeout_ms);
    if (err != RdKafka::ERR_NO_ERROR) {
        spdlog::warn("[kafka] flush timeout – {} messages may be lost",
                     producer_->outq_len());
    }
}

void KafkaProducer::DeliveryCb::dr_cb(RdKafka::Message& msg) {
    if (msg.err() != RdKafka::ERR_NO_ERROR) {
        metrics_.kafka_delivery_errors().Increment();
        spdlog::debug("[kafka] delivery error: {}", msg.errstr());
    }
}

} // namespace packetpipe
