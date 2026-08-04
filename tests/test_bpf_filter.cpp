#include <gtest/gtest.h>
#include <pcap/pcap.h>
#include <string>

// Tests that the BPF filter subsystem (via pcap) compiles valid expressions
// and rejects invalid ones.  Uses pcap_open_dead so no network access is needed.

TEST(BpfFilterTest, EmptyFilterCompiles) {
    pcap_t* handle = pcap_open_dead(DLT_EN10MB, 65535);
    ASSERT_NE(handle, nullptr);

    struct bpf_program fp{};
    // An empty filter string matches everything – should compile without error
    int rc = pcap_compile(handle, &fp, "", 1, PCAP_NETMASK_UNKNOWN);
    EXPECT_EQ(rc, 0) << "pcap_compile: " << pcap_geterr(handle);
    pcap_freecode(&fp);
    pcap_close(handle);
}

TEST(BpfFilterTest, TcpPort80FilterCompiles) {
    pcap_t* handle = pcap_open_dead(DLT_EN10MB, 65535);
    ASSERT_NE(handle, nullptr);

    struct bpf_program fp{};
    int rc = pcap_compile(handle, &fp, "tcp port 80", 1, PCAP_NETMASK_UNKNOWN);
    EXPECT_EQ(rc, 0) << "pcap_compile: " << pcap_geterr(handle);
    pcap_freecode(&fp);
    pcap_close(handle);
}

TEST(BpfFilterTest, UdpHostFilterCompiles) {
    pcap_t* handle = pcap_open_dead(DLT_EN10MB, 65535);
    ASSERT_NE(handle, nullptr);

    struct bpf_program fp{};
    int rc = pcap_compile(handle, &fp, "udp and host 192.168.1.1", 1, PCAP_NETMASK_UNKNOWN);
    EXPECT_EQ(rc, 0) << "pcap_compile: " << pcap_geterr(handle);
    pcap_freecode(&fp);
    pcap_close(handle);
}

TEST(BpfFilterTest, InvalidFilterFails) {
    pcap_t* handle = pcap_open_dead(DLT_EN10MB, 65535);
    ASSERT_NE(handle, nullptr);

    struct bpf_program fp{};
    int rc = pcap_compile(handle, &fp, "this_is_not_a_valid_bpf_expression !!!",
                          1, PCAP_NETMASK_UNKNOWN);
    EXPECT_NE(rc, 0) << "Expected compile failure for invalid BPF expression";
    pcap_close(handle);
}

TEST(BpfFilterTest, IcmpFilterCompiles) {
    pcap_t* handle = pcap_open_dead(DLT_EN10MB, 65535);
    ASSERT_NE(handle, nullptr);

    struct bpf_program fp{};
    int rc = pcap_compile(handle, &fp, "icmp", 1, PCAP_NETMASK_UNKNOWN);
    EXPECT_EQ(rc, 0) << "pcap_compile: " << pcap_geterr(handle);
    pcap_freecode(&fp);
    pcap_close(handle);
}
