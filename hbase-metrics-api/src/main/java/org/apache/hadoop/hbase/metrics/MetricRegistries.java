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

package org.apache.hadoop.hbase.metrics;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * MetricRegistries is collection of MetricRegistry's. MetricsRegistries implementations should do
 * ref-counting of MetricRegistry's via create() and remove() methods.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public abstract class MetricRegistries {

  private static final class LazyHolder {
    private static final MetricRegistries GLOBAL = MetricRegistriesLoader.load();
  }

  /**
   * Return the global singleton instance for the MetricRegistries.
   * @return MetricRegistries implementation.
   */
  public static MetricRegistries global() {
    return LazyHolder.GLOBAL;
  }

  /**
   * Removes all the MetricRegisties.
   */
  public abstract void clear();

  /**
   * Create or return MetricRegistry with the given info. MetricRegistry will only be created
   * if current reference count is 0. Otherwise ref counted is incremented, and an existing instance
   * will be returned.
   * @param info the info object for the MetricRegistrytry.
   * @return created or existing MetricRegistry.
   */
  public abstract MetricRegistry create(MetricRegistryInfo info);

  /**
   * Decrements the ref count of the MetricRegistry, and removes if ref count == 0.
   * @param key the info object for the MetricRegistrytry.
   * @return true if metric registry is removed.
   */
  public abstract boolean remove(MetricRegistryInfo key);

  /**
   * Returns the MetricRegistry if found.
   * @param info the info for the registry.
   * @return a MetricRegistry optional.
   */
  public abstract Optional<MetricRegistry> get(MetricRegistryInfo info);

  /**
   * Returns MetricRegistryInfo's for the MetricRegistry's created.
   * @return MetricRegistryInfo's for the MetricRegistry's created.
   */
  public abstract Set<MetricRegistryInfo> getMetricRegistryInfos();

  /**
   * Returns MetricRegistry's created.
   * @return MetricRegistry's created.
   */
  public abstract Collection<MetricRegistry> getMetricRegistries();
}
