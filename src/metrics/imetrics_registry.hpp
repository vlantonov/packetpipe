#pragma once
#include "events/flow_event.hpp"
#include <prometheus/counter.h>
#include <prometheus/gauge.h>

namespace packetpipe {

/// Abstract interface for all Prometheus metric handles.
/// Injected into modules at construction time; outlives all module objects.
class IMetricsRegistry {
public:
    virtual ~IMetricsRegistry() = default;

    virtual prometheus::Counter& packets_received()             = 0;
    virtual prometheus::Counter& packets_dropped()              = 0;
    virtual prometheus::Gauge&   flows_active()                 = 0;
    virtual prometheus::Counter& flows_created()                = 0;
    /// @param r Determines which labelled counter variant is returned.
    virtual prometheus::Counter& flows_expired(ExpireReason r)  = 0;
    virtual prometheus::Counter& kafka_messages_produced()      = 0;
    virtual prometheus::Counter& kafka_delivery_errors()        = 0;
    virtual prometheus::Counter& avro_serialization_errors()    = 0;
    virtual prometheus::Counter& schema_registry_retries()      = 0;
};

} // namespace packetpipe
