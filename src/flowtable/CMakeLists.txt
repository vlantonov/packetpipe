add_library(packetpipe_flowtable STATIC flow_table.cpp)
target_include_directories(packetpipe_flowtable PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(packetpipe_flowtable
    PUBLIC
        packetpipe_metrics
    PRIVATE
        packetpipe_packets
        packetpipe_events
        spdlog::spdlog
)
