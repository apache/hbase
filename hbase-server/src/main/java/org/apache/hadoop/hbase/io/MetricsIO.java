/*
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
package org.apache.hadoop.hbase.io;

import com.google.errorprone.annotations.RestrictedApi;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MetricsIO {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsIO.class);

  /**
   * Shared instance returned while IO metrics are disabled. It discards all updates and never
   * registers anything with the metrics system.
   */
  private static final MetricsIO DISABLED_INSTANCE =
    new MetricsIO(new NoOpMetricsIOSource(), new MetricsIOWrapperImpl());

  private static volatile boolean enabled = false;
  private static volatile MetricsIO instance;
  private final MetricsIOSource source;
  private final MetricsIOWrapper wrapper;

  @RestrictedApi(explanation = "Should only be called in TestMetricsIO", link = "",
      allowedOnPath = ".*/(MetricsIO|TestMetricsIO).java")
  MetricsIO(MetricsIOWrapper wrapper) {
    this(CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
      .createIO(wrapper), wrapper);
  }

  MetricsIO(MetricsIOSource source, MetricsIOWrapper wrapper) {
    this.source = source;
    this.wrapper = wrapper;
  }

  /**
   * Enables IO metrics for this process. Until this is called, {@link #getInstance()} returns an
   * instance that discards all updates, so processes that only read or write HFiles as a client
   * (bulk load, MapReduce tasks, command line tools) do not start the Hadoop metrics system or
   * register a "RegionServer" metrics source. Server processes call this during startup. The
   * metrics source itself is still created lazily, on first use after this call. See HBASE-30231
   * for more details.
   */
  public static void enable() {
    enabled = true;
  }

  /** Returns whether IO metrics have been enabled for this process. */
  public static boolean isEnabled() {
    return enabled;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public static synchronized void resetForTesting() {
    enabled = false;
    instance = null;
  }

  /**
   * Get a static instance for the MetricsIO so that accessors access the same instance. We want to
   * lazy initialize so that correct process name is in place. See HBASE-27966 for more details.
   * <p>
   * If IO metrics have not been enabled with {@link #enable()}, a shared instance that discards all
   * updates is returned. If the metrics source cannot be created, a no-op source is used instead,
   * so that a metrics failure never fails HFile reads or writes. See HBASE-30231 for more details.
   */
  public static MetricsIO getInstance() {
    if (!enabled) {
      return DISABLED_INSTANCE;
    }
    if (instance == null) {
      synchronized (MetricsIO.class) {
        if (instance == null) {
          MetricsIOWrapper wrapper = new MetricsIOWrapperImpl();
          instance = createInstance(() -> new MetricsIO(wrapper), wrapper);
        }
      }
    }
    return instance;
  }

  /**
   * Creates a {@link MetricsIO} with the given supplier, falling back to one backed by a no-op
   * {@link MetricsIOSource} if the supplier fails.
   * <p>
   * Registering the metrics source with the Hadoop metrics system reserves its name before the
   * registration has fully completed. If anything fails after that point, every later attempt to
   * register the same source fails with "Metrics source ... already exists!". Retrying would
   * therefore fail on every call for the rest of the JVM's life, so we fall back to a no-op source
   * once and never retry.
   */
  static MetricsIO createInstance(Supplier<MetricsIO> supplier, MetricsIOWrapper wrapper) {
    try {
      return supplier.get();
    } catch (RuntimeException | LinkageError e) {
      LOG.warn("Failed to initialize the {} metrics source; FileSystem IO metrics will not be"
        + " reported by this process", MetricsIOSource.METRICS_JMX_CONTEXT, e);
      return new MetricsIO(new NoOpMetricsIOSource(), wrapper);
    }
  }

  public MetricsIOSource getMetricsSource() {
    return source;
  }

  public MetricsIOWrapper getWrapper() {
    return wrapper;
  }

  public void updateFsReadTime(long t) {
    source.updateFsReadTime(t);
  }

  public void updateFsPreadTime(long t) {
    source.updateFsPReadTime(t);
  }

  public void updateFsWriteTime(long t) {
    source.updateFsWriteTime(t);
  }

  public void incrSlowFsRead() {
    source.incrSlowFsRead();
  }

  /**
   * A {@link MetricsIOSource} that discards all updates. Used while IO metrics are disabled, or
   * when the real metrics source could not be created.
   */
  static final class NoOpMetricsIOSource implements MetricsIOSource {

    @Override
    public void updateFsReadTime(long t) {
    }

    @Override
    public void updateFsPReadTime(long t) {
    }

    @Override
    public void updateFsWriteTime(long t) {
    }

    @Override
    public void incrSlowFsRead() {
    }

    @Override
    public void init() {
    }

    @Override
    public void setGauge(String gaugeName, long value) {
    }

    @Override
    public void incGauge(String gaugeName, long delta) {
    }

    @Override
    public void decGauge(String gaugeName, long delta) {
    }

    @Override
    public void removeMetric(String key) {
    }

    @Override
    public void incCounters(String counterName, long delta) {
    }

    @Override
    public void updateHistogram(String name, long value) {
    }

    @Override
    public String getMetricsContext() {
      return METRICS_CONTEXT;
    }

    @Override
    public String getMetricsDescription() {
      return METRICS_DESCRIPTION;
    }

    @Override
    public String getMetricsJmxContext() {
      return METRICS_JMX_CONTEXT;
    }

    @Override
    public String getMetricsName() {
      return METRICS_NAME;
    }
  }
}
