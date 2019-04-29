/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Gauge;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example coprocessor that collects some metrics to demonstrate the usage of exporting custom
 * metrics from the coprocessor.
 *
 * <p>
 * These metrics will be available through the regular Hadoop metrics2 sinks (ganglia, opentsdb,
 * etc) as well as JMX output. You can view a snapshot of the metrics by going to the http web UI
 * of the master page, something like http://mymasterhost:16010/jmx
 * </p>
 * @see ExampleRegionObserverWithMetrics
 */
@InterfaceAudience.Private
public class ExampleMasterObserverWithMetrics implements MasterCoprocessor, MasterObserver {
  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  private static final Logger LOG = LoggerFactory.getLogger(ExampleMasterObserverWithMetrics.class);

  /** This is the Timer metric object to keep track of the current count across invocations */
  private Timer createTableTimer;
  private long createTableStartTime = Long.MIN_VALUE;

  /** This is a Counter object to keep track of disableTable operations */
  private Counter disableTableCounter;

  /** Returns the total memory of the process. We will use this to define a gauge metric */
  private long getTotalMemory() {
    return Runtime.getRuntime().totalMemory();
  }

  /** Returns the max memory of the process. We will use this to define a gauge metric */
  private long getMaxMemory() {
    return Runtime.getRuntime().maxMemory();
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             TableDescriptor desc, RegionInfo[] regions) throws IOException {
    // we rely on the fact that there is only 1 instance of our MasterObserver. We keep track of
    // when the operation starts before the operation is executing.
    this.createTableStartTime = System.currentTimeMillis();
  }

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableDescriptor desc, RegionInfo[] regions) throws IOException {
    if (this.createTableStartTime > 0) {
      long time = System.currentTimeMillis() - this.createTableStartTime;
      LOG.info("Create table took: " + time);

      // Update the timer metric for the create table operation duration.
      createTableTimer.updateMillis(time);
    }
  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    // Increment the Counter for disable table operations
    this.disableTableCounter.increment();
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    // start for the MasterObserver will be called only once in the lifetime of the
    // server. We will construct and register all metrics that we will track across method
    // invocations.

    if (env instanceof MasterCoprocessorEnvironment) {
      // Obtain the MetricRegistry for the Master. Metrics from this registry will be reported
      // at the master level per-server.
      MetricRegistry registry =
          ((MasterCoprocessorEnvironment) env).getMetricRegistryForMaster();

      if (createTableTimer == null) {
        // Create a new Counter, or get the already registered counter.
        // It is much better to only call this once and save the Counter as a class field instead
        // of creating the counter every time a coprocessor method is invoked. This will negate
        // any performance bottleneck coming from map lookups tracking metrics in the registry.
        createTableTimer = registry.timer("CreateTable");

        // on stop(), we can remove these registered metrics via calling registry.remove(). But
        // it is not needed for coprocessors at the master level. If coprocessor is stopped,
        // the server is stopping anyway, so there will not be any resource leaks.
      }

      if (disableTableCounter == null) {
        disableTableCounter = registry.counter("DisableTable");
      }

      // Register a custom gauge. The Gauge object will be registered in the metrics registry and
      // periodically the getValue() is invoked to obtain the snapshot.
      registry.register("totalMemory", new Gauge<Long>() {
        @Override
        public Long getValue() {
          return getTotalMemory();
        }
      });

      // Register a custom gauge using Java-8 lambdas (Supplier converted into Gauge)
      registry.register("maxMemory", this::getMaxMemory);
    }
  }
}
