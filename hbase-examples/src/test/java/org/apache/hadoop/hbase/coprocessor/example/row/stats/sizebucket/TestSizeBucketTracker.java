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
package org.apache.hadoop.hbase.coprocessor.example.row.stats.sizebucket;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.SizeBucket;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.SizeBucketTracker;
import org.junit.Test;

import org.apache.hbase.thirdparty.com.google.gson.JsonObject;

public class TestSizeBucketTracker {

  @Test
  public void itUpdatesSizeBuckets() {
    SizeBucketTracker sizeBucketTracker = new SizeBucketTracker();
    SizeBucket[] sizeBuckets = SizeBucket.values();

    // Initialize
    Map<String, Long> bucketToCount = sizeBucketTracker.toMap();
    for (SizeBucket sizeBucket : SizeBucket.values()) {
      assertEquals((long) bucketToCount.get(sizeBucket.bucket()), 0L);
    }

    // minBytes
    for (SizeBucket sizeBucket : sizeBuckets) {
      sizeBucketTracker.add(sizeBucket.minBytes());
    }
    bucketToCount = sizeBucketTracker.toMap();
    for (SizeBucket sizeBucket : sizeBuckets) {
      assertEquals((long) bucketToCount.get(sizeBucket.bucket()), 1L);
    }

    // maxBytes - 1
    for (SizeBucket sizeBucket : sizeBuckets) {
      sizeBucketTracker.add(sizeBucket.maxBytes() - 1);
    }
    bucketToCount = sizeBucketTracker.toMap();
    for (SizeBucket sizeBucket : sizeBuckets) {
      assertEquals((long) bucketToCount.get(sizeBucket.bucket()), 2L);
    }

    // maxBytes
    for (SizeBucket sizeBucket : sizeBuckets) {
      sizeBucketTracker.add(sizeBucket.maxBytes());
    }
    bucketToCount = sizeBucketTracker.toMap();
    for (int i = 0; i < sizeBuckets.length - 1; i++) {
      SizeBucket currBucket = sizeBuckets[i];
      if (currBucket == SizeBucket.KILOBYTES_1) {
        assertEquals((long) bucketToCount.get(currBucket.bucket()), 2L);
      } else {
        SizeBucket nextBucket = sizeBuckets[i + 1];
        if (nextBucket == SizeBucket.KILOBYTES_MAX) {
          assertEquals((long) bucketToCount.get(nextBucket.bucket()), 4L);
        } else {
          assertEquals((long) bucketToCount.get(nextBucket.bucket()), 3L);
        }
      }
    }
  }

  @Test
  public void itCreatesJson() {
    SizeBucketTracker sizeBucketTracker = new SizeBucketTracker();
    SizeBucket[] sizeBuckets = SizeBucket.values();
    for (SizeBucket sizeBucket : sizeBuckets) {
      sizeBucketTracker.add(sizeBucket.minBytes());
    }
    JsonObject mapJson = sizeBucketTracker.toJsonObject();
    for (SizeBucket sizeBucket : sizeBuckets) {
      Number count = mapJson.get(sizeBucket.bucket()).getAsNumber();
      assertEquals(count.longValue(), 1L);
    }
  }
}
