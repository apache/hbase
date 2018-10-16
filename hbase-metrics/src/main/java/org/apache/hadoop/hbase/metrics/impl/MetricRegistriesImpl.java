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
package org.apache.hadoop.hbase.metrics.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryFactory;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementation of MetricRegistries that does ref-counting.
 */
@InterfaceAudience.Private
public class MetricRegistriesImpl extends MetricRegistries {
  private final MetricRegistryFactory factory;
  private final RefCountingMap<MetricRegistryInfo, MetricRegistry> registries;

  public MetricRegistriesImpl() {
    this(new MetricRegistryFactoryImpl());
  }

  public MetricRegistriesImpl(MetricRegistryFactory factory) {
    this.factory = factory;
    this.registries = new RefCountingMap<>();
  }

  @Override
  public MetricRegistry create(MetricRegistryInfo info) {
    return registries.put(info, () -> factory.create(info));
  }

  @Override
  public boolean remove(MetricRegistryInfo key) {
    return registries.remove(key) == null;
  }

  @Override
  public Optional<MetricRegistry> get(MetricRegistryInfo info) {
    return Optional.ofNullable(registries.get(info));
  }

  @Override
  public Collection<MetricRegistry> getMetricRegistries() {
    return Collections.unmodifiableCollection(registries.values());
  }

  @Override
  public void clear() {
    registries.clear();
  }

  @Override
  public Set<MetricRegistryInfo> getMetricRegistryInfos() {
    return Collections.unmodifiableSet(registries.keySet());
  }
}
