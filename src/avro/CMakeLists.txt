add_library(packetpipe_avro STATIC
    schema_registry_client.cpp
    avro_serializer.cpp
)
target_include_directories(packetpipe_avro PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(packetpipe_avro
    PUBLIC
        packetpipe_events
        avro::avrocpp
    PRIVATE
        httplib::httplib
        nlohmann_json::nlohmann_json
)
