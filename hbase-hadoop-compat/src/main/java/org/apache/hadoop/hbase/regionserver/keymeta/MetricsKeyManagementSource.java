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
package org.apache.hadoop.hbase.regionserver.keymeta;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface for metrics about HBase Key Management. Implementations will expose these metrics via
 * JMX and Hadoop Metrics2 sinks.
 */
@InterfaceAudience.Private
public interface MetricsKeyManagementSource extends BaseSource {

  String METRICS_NAME = "KeyManagement";
  String METRICS_CONTEXT = "regionserver";
  String METRICS_DESCRIPTION = "Metrics about HBase Key Management";
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  // Write-path counter names
  String WRITE_KEY_LOOKUP_REQUESTS = "writeKeyLookupRequests";
  String WRITE_KEY_LOOKUP_REQUESTS_DESC = "Total write path key lookup calls";
  String WRITE_KEY_LOOKUP_CACHE_HIT = "writeKeyLookupCacheHit";
  String WRITE_KEY_LOOKUP_CACHE_HIT_DESC = "Write path L1 cache hits";
  String WRITE_KEY_LOOKUP_CACHE_MISS = "writeKeyLookupCacheMiss";
  String WRITE_KEY_LOOKUP_CACHE_MISS_DESC = "Write path L1 cache misses (loader invoked)";
  String WRITE_KEY_LOOKUP_RESULT_USABLE = "writeKeyLookupResultUsable";
  String WRITE_KEY_LOOKUP_RESULT_USABLE_DESC = "Write path lookups that returned a usable key";
  String WRITE_KEY_LOOKUP_RESULT_DISABLED = "writeKeyLookupResultDisabled";
  String WRITE_KEY_LOOKUP_RESULT_DISABLED_DESC = "Write path lookups that hit a disabled key";

  // Read-path counter names
  String READ_KEY_LOOKUP_REQUESTS = "readKeyLookupRequests";
  String READ_KEY_LOOKUP_REQUESTS_DESC = "Total read path key lookup calls";
  String READ_KEY_LOOKUP_CACHE_HIT = "readKeyLookupCacheHit";
  String READ_KEY_LOOKUP_CACHE_HIT_DESC = "Read path L1 cache hits";
  String READ_KEY_LOOKUP_CACHE_MISS = "readKeyLookupCacheMiss";
  String READ_KEY_LOOKUP_CACHE_MISS_DESC = "Read path L1 cache misses (loader invoked)";
  String READ_KEY_LOOKUP_RESULT_USABLE = "readKeyLookupResultUsable";
  String READ_KEY_LOOKUP_RESULT_USABLE_DESC = "Read path lookups that returned a usable key";
  String READ_KEY_LOOKUP_RESULT_DISABLED = "readKeyLookupResultDisabled";
  String READ_KEY_LOOKUP_RESULT_DISABLED_DESC = "Read path lookups that hit a disabled key";

  // Provider counter names
  String PROVIDER_CALL_COUNT = "providerCallCount";
  String PROVIDER_CALL_COUNT_DESC = "Total KMS provider API calls";
  String PROVIDER_CALL_TIME = "providerCallTime";
  String PROVIDER_CALL_TIME_DESC = "KMS provider API call latency";
  String PROVIDER_CALL_FAILURES = "providerCallFailures";
  String PROVIDER_CALL_FAILURES_DESC = "Failed KMS provider API calls";

  // Cache gauge names
  String KEY_CACHE_SIZE = "keyCacheSize";
  String KEY_CACHE_SIZE_DESC = "Estimated entry count in the identity key cache";
  String KEY_CACHE_ACTIVE_SIZE = "keyCacheActiveSize";
  String KEY_CACHE_ACTIVE_SIZE_DESC = "Estimated entry count in the active key cache";
  String KEY_CACHE_USABLE_COUNT = "keyCacheUsableCount";
  String KEY_CACHE_USABLE_COUNT_DESC =
    "Count of usable (ACTIVE or INACTIVE) entries in the identity key cache";

  /**
   * Registers cache-related counters and gauges. Must be called once on regionserver before any
   * cache metric methods are used. On master, this is never called and cache metrics won't exist.
   */
  void registerCacheMetrics();

  // Write-path mutators
  void incrementWriteKeyLookupRequests();

  void incrementWriteKeyLookupCacheHit();

  void incrementWriteKeyLookupCacheMiss();

  void incrementWriteKeyLookupResultUsable();

  void incrementWriteKeyLookupResultDisabled();

  // Read-path mutators
  void incrementReadKeyLookupRequests();

  void incrementReadKeyLookupCacheHit();

  void incrementReadKeyLookupCacheMiss();

  void incrementReadKeyLookupResultUsable();

  void incrementReadKeyLookupResultDisabled();

  // Provider mutators
  void incrementProviderCallCount();

  void updateProviderCallTime(long timeMs);

  void incrementProviderCallFailures();

  // Cache gauge mutators
  void setKeyCacheSize(long size);

  void setKeyCacheActiveSize(long size);

  void setKeyCacheUsableCount(long count);
}
