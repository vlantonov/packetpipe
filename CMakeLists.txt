cmake_minimum_required(VERSION 3.25)

project(packetpipe VERSION 0.1.1 LANGUAGES CXX)

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

# vcpkg's avro-cpp config may expose only avro-cpp::avrocpp_s (static) while
# still creating aliases that expect avro-cpp::avrocpp. Predeclare the target
# so config-time alias creation does not fail in static-linkage environments.
if(NOT TARGET avro-cpp::avrocpp)
    add_library(avro-cpp::avrocpp INTERFACE IMPORTED)
endif()

find_package(avro-cpp CONFIG REQUIRED)

if(NOT TARGET avro::avrocpp)
    add_library(avro::avrocpp INTERFACE IMPORTED)

    if(TARGET avro-cpp::avrocpp_s)
        target_link_libraries(avro::avrocpp INTERFACE avro-cpp::avrocpp_s)
    elseif(TARGET avro-cpp::avrocpp)
        target_link_libraries(avro::avrocpp INTERFACE avro-cpp::avrocpp)
    elseif(TARGET avrocpp_s)
        target_link_libraries(avro::avrocpp INTERFACE avrocpp_s)
    elseif(TARGET avrocpp)
        target_link_libraries(avro::avrocpp INTERFACE avrocpp)
    else()
        message(FATAL_ERROR "Could not resolve Avro C++ target from avro-cpp package.")
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
