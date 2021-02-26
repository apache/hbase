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

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class MetricRegistriesLoader {
  private static final Logger LOG = LoggerFactory.getLogger(MetricRegistries.class);

  private static final String defaultClass
      = "org.apache.hadoop.hbase.metrics.impl.MetricRegistriesImpl";

  private MetricRegistriesLoader() {
  }

  /**
   * Creates a {@link MetricRegistries} instance using the corresponding {@link MetricRegistries}
   * available to {@link ServiceLoader} on the classpath. If no instance is found, then default
   * implementation will be loaded.
   * @return A {@link MetricRegistries} implementation.
   */
  public static MetricRegistries load() {
    List<MetricRegistries> availableImplementations = getDefinedImplemantations();
    return load(availableImplementations);
  }

  /**
   * Creates a {@link MetricRegistries} instance using the corresponding {@link MetricRegistries}
   * available to {@link ServiceLoader} on the classpath. If no instance is found, then default
   * implementation will be loaded.
   * @return A {@link MetricRegistries} implementation.
   */
  static MetricRegistries load(List<MetricRegistries> availableImplementations) {

    if (availableImplementations.size() == 1) {
      // One and only one instance -- what we want/expect
      MetricRegistries impl = availableImplementations.get(0);
      LOG.info("Loaded MetricRegistries " + impl.getClass());
      return impl;
    } else if (availableImplementations.isEmpty()) {
      try {
        return ReflectionUtils.newInstance((Class<MetricRegistries>)Class.forName(defaultClass));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    } else {
      // Tell the user they're doing something wrong, and choose the first impl.
      StringBuilder sb = new StringBuilder();
      for (MetricRegistries factory : availableImplementations) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append(factory.getClass());
      }
      LOG.warn("Found multiple MetricRegistries implementations: " + sb
          + ". Using first found implementation: " + availableImplementations.get(0));
      return availableImplementations.get(0);
    }
  }

  private static List<MetricRegistries> getDefinedImplemantations() {
    ServiceLoader<MetricRegistries> loader = ServiceLoader.load(MetricRegistries.class);
    List<MetricRegistries> availableFactories = new ArrayList<>();
    for (MetricRegistries impl : loader) {
      availableFactories.add(impl);
    }
    return availableFactories;
  }
}
