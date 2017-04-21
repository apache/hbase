/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.Timer;

/**
 * An example coprocessor that collects some metrics to demonstrate the usage of exporting custom
 * metrics from the coprocessor.
 * <p>
 * These metrics will be available through the regular Hadoop metrics2 sinks (ganglia, opentsdb,
 * etc) as well as JMX output. You can view a snapshot of the metrics by going to the http web UI
 * of the regionserver page, something like http://myregionserverhost:16030/jmx
 * </p>
 *
 * @see ExampleMasterObserverWithMetrics
 */
public class ExampleRegionObserverWithMetrics implements RegionObserver {

  private Counter preGetCounter;
  private Timer costlyOperationTimer;

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
      throws IOException {
    // Increment the Counter whenever the coprocessor is called
    preGetCounter.increment();
  }

  @Override
  public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
                        List<Cell> results) throws IOException {
    // do a costly (high latency) operation which we want to measure how long it takes by
    // using a Timer (which is a Meter and a Histogram).
    long start = System.nanoTime();
    try {
      performCostlyOperation();
    } finally {
      costlyOperationTimer.updateNanos(System.nanoTime() - start);
    }
  }

  private void performCostlyOperation() {
    try {
      // simulate the operation by sleeping.
      Thread.sleep(ThreadLocalRandom.current().nextLong(100));
    } catch (InterruptedException ignore) {}
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    // start for the RegionServerObserver will be called only once in the lifetime of the
    // server. We will construct and register all metrics that we will track across method
    // invocations.

    if (env instanceof RegionCoprocessorEnvironment) {
      // Obtain the MetricRegistry for the RegionServer. Metrics from this registry will be reported
      // at the region server level per-regionserver.
      MetricRegistry registry =
          ((RegionCoprocessorEnvironment) env).getMetricRegistryForRegionServer();

      if (preGetCounter == null) {
        // Create a new Counter, or get the already registered counter.
        // It is much better to only call this once and save the Counter as a class field instead
        // of creating the counter every time a coprocessor method is invoked. This will negate
        // any performance bottleneck coming from map lookups tracking metrics in the registry.
        // Returned counter instance is shared by all coprocessors of the same class in the same
        // region server.
        preGetCounter = registry.counter("preGetRequests");
      }

      if (costlyOperationTimer == null) {
        // Create a Timer to track execution times for the costly operation.
        costlyOperationTimer = registry.timer("costlyOperation");
      }
    }
  }

  @Override
  public void stop(CoprocessorEnvironment e) throws IOException {
    // we should NOT remove / deregister the metrics in stop(). The whole registry will be
    // removed when the last region of the table is closed.
  }
}
