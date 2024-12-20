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
