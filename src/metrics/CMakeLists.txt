add_library(packetpipe_metrics STATIC metrics_registry.cpp)
target_include_directories(packetpipe_metrics PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(packetpipe_metrics
    PUBLIC
        packetpipe_events
        prometheus-cpp::core
    PRIVATE
        prometheus-cpp::pull
)
