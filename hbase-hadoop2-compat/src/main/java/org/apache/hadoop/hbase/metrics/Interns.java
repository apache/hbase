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

package org.apache.hadoop.hbase.metrics;



import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;


/**
 * Helpers to create interned metrics info
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class Interns {

  private static LoadingCache<String, ConcurrentHashMap<String, MetricsInfo>> infoCache =
      CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS)
          .build(new CacheLoader<String, ConcurrentHashMap<String, MetricsInfo>>() {
            public ConcurrentHashMap<String, MetricsInfo> load(String key) {
              return new ConcurrentHashMap<String, MetricsInfo>();
            }
          });
  private static LoadingCache<MetricsInfo, ConcurrentHashMap<String, MetricsTag>> tagCache =
      CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS)
          .build(new CacheLoader<MetricsInfo, ConcurrentHashMap<String, MetricsTag>>() {
            public ConcurrentHashMap<String, MetricsTag> load(MetricsInfo key) {
              return new ConcurrentHashMap<String, MetricsTag>();
            }
          });

  private Interns(){}

  /**
   * Get a metric info object
   *
   * @return an interned metric info object
   */
  public static MetricsInfo info(String name, String description) {
    Map<String, MetricsInfo> map = infoCache.getUnchecked(name);
    MetricsInfo info = map.get(description);
    if (info == null) {
      info = new MetricsInfoImpl(name, description);
      map.put(description, info);
    }
    return info;
  }

  /**
   * Get a metrics tag
   *
   * @param info  of the tag
   * @param value of the tag
   * @return an interned metrics tag
   */
  public static MetricsTag tag(MetricsInfo info, String value) {
    Map<String, MetricsTag> map = tagCache.getUnchecked(info);
    MetricsTag tag = map.get(value);
    if (tag == null) {
      tag = new MetricsTag(info, value);
      map.put(value, tag);
    }
    return tag;
  }

  /**
   * Get a metrics tag
   *
   * @param name        of the tag
   * @param description of the tag
   * @param value       of the tag
   * @return an interned metrics tag
   */
  public static MetricsTag tag(String name, String description, String value) {
    return tag(info(name, description), value);
  }
}