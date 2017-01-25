Overview
========
hbase-metrics and hbase-metrics-api are two modules that define and implement the "new" metric
system used internally within HBase. These two modules (and some other code in hbase-hadoop2-compat)
module are referred as "HBase metrics framework".

HBase-metrics-api Module
========================
HBase Metrics API (hbase-metrics-api) contains the interface
that HBase exposes internally and to third party code (including coprocessors). It is a thin
abstraction over the actual implementation for backwards compatibility guarantees. The source
/ binary and other compatibility guarantees are for "LimitedPrivate API" (see [1] for an
explanation).

The metrics API in this hbase-metrics-api module is inspired by the Dropwizard metrics 3.1 API
(See [2]). It is a subset of the API only containing metrics collection. However, the implementation
is HBase-specific and provided in hbase-metrics module. All of the classes in this module is
HBase-internal. See the latest documentation of Dropwizard metrics for examples of defining / using
metrics.


HBase-metrics Module
====================
hbase-metrics module contains implementation of the "HBase Metrics API", including MetricRegistry,
Counter, Histogram, etc. These are highly concurrent implementations of the Metric interfaces.
Metrics in HBase are grouped into different sets (like WAL, RPC, RegionServer, etc). Each group of
metrics should be tracked via a MetricRegistry specific to that group. Metrics can be dynamically
added or removed from the registry with a name. Each Registry is independent of the other
registries and will have it's own JMX context and MetricRecord (when used with Metrics2).


MetricRegistry's themselves are tracked via a global registry (of MetricRegistries) called
MetricRegistries. MetricRegistries.global() can be used to obtain the global instance.
MetricRegistry instances can also be dynamically registered and removed. However, unlike the
MetricRegistry, MetricRegistries does reference counting of the MetricRegistry instances. Only
Metrics in the MetricRegistry instances that are in the global MetricRegistry are exported to the
metric sinks or JMX.


Coprocessor Metrics
===================
HBase allows custom coprocessors to track and export metrics using the new framework.
Coprocessors and other third party code should only use the classes and interfaces from
hbase-metrics-api module and only the classes that are marked with InterfaceAudience.LimitedPrivate
annotation. There is no guarantee on the compatibility requirements for other classes.

Coprocessors can obtain the MetricRegistry to register their custom metrics via corresponding
CoprocessorEnvironment context. See ExampleRegionObserverWithMetrics and
ExampleMasterObserverWithMetrics classes in hbase-examples module for usage.


Developer Notes
===============
Historically, HBase has been using Hadoop's Metrics2 framework [3] for collecting and reporting the
metrics internally. However, due to the difficultly of dealing with the Metrics2 framework, HBase is
moving away from Hadoop's metrics implementation to its custom implementation. The move will happen
incrementally, and during the time, both Hadoop Metrics2-based metrics and hbase-metrics module
based classes will be in the source code. All new implementations for metrics SHOULD use the new
API and framework.

Examples of the new framework can be found in MetricsCoprocessor and MetricsRegionServerSourceImpl
classes. See HBASE-9774 [4] for more context.

hbase-metrics module right now only deals with metrics tracking and collection. It does not do JMX
reporting or reporting to console, ganglia, opentsdb, etc. We use Hadoop's Metrics2 for reporting
metrics to different sinks or exporting via JMX. However, this is contained within the
hbase-hadoop2-compat module completely, so that rest of the code does not know anything about the
Metrics2 dependency. HBaseMetrics2HadoopMetricsAdapter is the adapter that can collect metrics
in a MetricRegistry using the metric2 MetricsCollector / MetricRecordBuilder interfaces.
GlobalMetricRegistriesSource is the global Metrics2 Source that collects all of the metrics in all
of the metric registries in the MetricRegistries.global() instance.


References
1. https://hbase.apache.org/book.html#hbase.versioning
2. http://metrics.dropwizard.io/
3. https://hadoop.apache.org/docs/r2.7.2/api/org/apache/hadoop/metrics2/package-summary.html
4. https://issues.apache.org/jira/browse/HBASE-9774