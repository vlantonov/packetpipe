add_library(packetpipe_kafka STATIC
    kafka_producer.cpp
    avro_kafka_sink.cpp
)
target_include_directories(packetpipe_kafka PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(packetpipe_kafka
    PUBLIC
        packetpipe_avro
        packetpipe_events
        packetpipe_metrics
        RdKafka::rdkafka++
    PRIVATE
        spdlog::spdlog
)
