find_package(GTest CONFIG REQUIRED)

add_executable(packetpipe_tests
    test_config_parser.cpp
    test_flow_key.cpp
    test_flow_table.cpp
    test_kafka_key.cpp
    test_avro_roundtrip.cpp
    test_bpf_filter.cpp
    test_packet_decoder.cpp
)

target_link_libraries(packetpipe_tests
    PRIVATE
        packetpipe_config
        packetpipe_events
        packetpipe_flowtable
        packetpipe_packets
        packetpipe_avro
        packetpipe_metrics
        GTest::gtest
        GTest::gtest_main
)

find_package(libpcap QUIET)
if(libpcap_FOUND)
    target_link_libraries(packetpipe_tests PRIVATE libpcap::libpcap)
else()
    find_package(pcap CONFIG QUIET)
    if(pcap_FOUND)
        target_link_libraries(packetpipe_tests PRIVATE pcap::pcap)
    else()
        find_package(PkgConfig REQUIRED)
        pkg_check_modules(LIBPCAP REQUIRED IMPORTED_TARGET libpcap)
        target_link_libraries(packetpipe_tests PRIVATE PkgConfig::LIBPCAP)
    endif()
endif()

target_compile_definitions(packetpipe_tests PRIVATE
    TEST_SCHEMA_PATH="${CMAKE_SOURCE_DIR}/schemas/FlowEvent.avsc"
)

include(GoogleTest)
gtest_discover_tests(packetpipe_tests)
