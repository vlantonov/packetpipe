cmake_minimum_required(VERSION 3.25)

project(packetpipe VERSION 0.1.3 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(PACKETPIPE_BUILD_TESTS "Build unit tests" ON)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

add_compile_definitions(PACKETPIPE_VERSION="${PROJECT_VERSION}")

find_package(spdlog CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(httplib CONFIG REQUIRED)
find_package(prometheus-cpp CONFIG REQUIRED)
find_package(RdKafka CONFIG REQUIRED)

# 1.11.x recipe uses package name "libavrocpp"; 1.12.x overrides to "avro-cpp"
find_package(libavrocpp QUIET)
if(NOT libavrocpp_FOUND)
    find_package(avro-cpp REQUIRED)
endif()

if(NOT TARGET avro::avrocpp)
    add_library(avro::avrocpp INTERFACE IMPORTED)
    if(TARGET libavrocpp::libavrocpp)
        target_link_libraries(avro::avrocpp INTERFACE libavrocpp::libavrocpp)
    elseif(TARGET avro-cpp::avrocpp)
        target_link_libraries(avro::avrocpp INTERFACE avro-cpp::avrocpp)
    elseif(TARGET avro-cpp::avrocpp_s)
        target_link_libraries(avro::avrocpp INTERFACE avro-cpp::avrocpp_s)
    else()
        message(FATAL_ERROR "Could not resolve Avro C++ target.")
    endif()
endif()

add_subdirectory(src/config)
add_subdirectory(src/events)
add_subdirectory(src/metrics)
add_subdirectory(src/packets)
add_subdirectory(src/flowtable)
add_subdirectory(src/avro)
add_subdirectory(src/kafka)

add_executable(packetpipe src/main.cpp)
target_compile_definitions(packetpipe PRIVATE PACKETPIPE_SCHEMA_DIR="${CMAKE_SOURCE_DIR}/schemas")
target_link_libraries(packetpipe
    PRIVATE
        packetpipe_config
        packetpipe_packets
        packetpipe_flowtable
        packetpipe_avro
        packetpipe_kafka
        packetpipe_metrics
        spdlog::spdlog
)

if(PACKETPIPE_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
