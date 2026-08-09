from conan import ConanFile


class PacketPipeConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"

    def requirements(self):
        self.requires("libavrocpp/1.11.3")
        self.requires("cpp-httplib/0.14.3")
        self.requires("libpcap/1.10.4")
        self.requires("librdkafka/2.14.2")
        self.requires("nlohmann_json/3.11.3")
        self.requires("prometheus-cpp/1.2.4")
        self.requires("spdlog/1.17.0")

    def build_requirements(self):
        self.test_requires("gtest/1.14.0")
