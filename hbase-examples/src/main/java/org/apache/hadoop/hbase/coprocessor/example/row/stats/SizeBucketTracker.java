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
package org.apache.hadoop.hbase.coprocessor.example.row.stats;

import java.util.HashMap;
import java.util.Map;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.gson.JsonObject;

@InterfaceAudience.Private
public class SizeBucketTracker {

  private static final SizeBucket[] SIZE_BUCKET_ARRAY = SizeBucket.values();
  private final Map<SizeBucket, Long> bucketToCount;

  public SizeBucketTracker() {
    SizeBucket[] sizeBucketsArray = SizeBucket.values();

    bucketToCount = new HashMap<>(sizeBucketsArray.length);
    for (SizeBucket sizeBucket : sizeBucketsArray) {
      bucketToCount.put(sizeBucket, 0L);
    }
  }

  public void add(long rowBytes) {
    if (rowBytes < 0) {
      return;
    }
    SizeBucket sizeBucket = search(rowBytes);
    if (sizeBucket == null) {
      return;
    }
    long val = bucketToCount.get(sizeBucket);
    bucketToCount.put(sizeBucket, getSafeIncrementedValue(val));
  }

  public Map<String, Long> toMap() {
    Map<String, Long> copy = new HashMap<>(SIZE_BUCKET_ARRAY.length);
    for (SizeBucket sizeBucket : SIZE_BUCKET_ARRAY) {
      long val = bucketToCount.get(sizeBucket);
      copy.put(sizeBucket.bucket(), val);
    }
    return copy;
  }

  public JsonObject toJsonObject() {
    JsonObject bucketJson = new JsonObject();
    for (SizeBucket sizeBucket : SIZE_BUCKET_ARRAY) {
      long val = bucketToCount.get(sizeBucket);
      bucketJson.addProperty(sizeBucket.bucket(), val);
    }
    return bucketJson;
  }

  private SizeBucket search(long val) {
    /*
     * Performance tested a few different search implementations 1. Linear 2. Binary 3. Search -
     * bucket search order changes over time as more information about the table is gained Linear
     * and Binary implementations had roughly similar throughput - Linear performs slightly better
     * when the sizes are small - Binary performs slightly better when the sizes are irregularly
     * distributed or skewed high Smart implementation had the lowest throughput - Reassessing the
     * bucket search order is an expensive operation - Tuning the number bucket search order
     * reassessments is tricky, since it depended on the - Write patterns to a table -- including
     * how hot/cold the compacting data is - Number of values per row - Number of rows per region -
     * Small number of SizeBucket values means that there is NOT a ton of value ot be gained from
     * reassessing the bucket search order Landed on Linear implementation because - Looping through
     * a small array is quick, especially if most of the values end up exiting out early - Many
     * tables at HubSpot have small values - Implementation is clear and requires no tuning PR with
     * more testing context: https://git.hubteam.com/HubSpot/HubSpotCoprocessors/pull/243
     */
    for (SizeBucket sizeBucket : SIZE_BUCKET_ARRAY) {
      if (val < sizeBucket.maxBytes()) {
        return sizeBucket;
      }
    }
    return val == Long.MAX_VALUE ? SizeBucket.KILOBYTES_MAX : null;
  }

  private static long getSafeIncrementedValue(long val) {
    return val == Long.MAX_VALUE ? val : val + 1;
  }
}
