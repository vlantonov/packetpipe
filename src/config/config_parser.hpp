#pragma once
#include "app_config.hpp"

namespace packetpipe {

/// Parses argc/argv into a validated AppConfig.
/// Throws std::invalid_argument on any invalid or missing argument.
class ConfigParser {
public:
    static AppConfig parse(int argc, char* argv[]);

private:
    ConfigParser() = delete;
};

} // namespace packetpipe
