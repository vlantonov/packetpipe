add_library(packetpipe_packets STATIC
    packet_decoder.cpp
    pcap_file_source.cpp
    live_capture_source.cpp
)
target_include_directories(packetpipe_packets PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)

# Prefer vcpkg-exported pcap target; fall back to system pkg-config
find_package(pcap CONFIG QUIET)
if(pcap_FOUND)
    target_link_libraries(packetpipe_packets PRIVATE pcap::pcap)
else()
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(LIBPCAP REQUIRED IMPORTED_TARGET libpcap)
    target_link_libraries(packetpipe_packets PRIVATE PkgConfig::LIBPCAP)
endif()

# libcap for CAP_NET_RAW check in live_capture_source.cpp
find_library(LIBCAP cap)
if(LIBCAP)
    target_link_libraries(packetpipe_packets PRIVATE ${LIBCAP})
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # Under some toolchain configurations (e.g., vcpkg), find_library may miss
    # system libcap even when it is installed. Link by name as a fallback.
    target_link_libraries(packetpipe_packets PRIVATE cap)
endif()
