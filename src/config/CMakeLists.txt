add_library(packetpipe_config STATIC config_parser.cpp)
target_include_directories(packetpipe_config PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_compile_definitions(packetpipe_config PUBLIC
    PACKETPIPE_VERSION="${PROJECT_VERSION}"
)
