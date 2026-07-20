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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.RateLimiter;

/**
 * Unit tests for bulkload copy bandwidth throttling in {@link HFileReplicator} and
 * {@link ReplicationSink}. Does not require a running HBase cluster.
 */
@Tag(ReplicationTests.TAG)
@Tag(SmallTests.TAG)
public class TestHFileReplicatorBandwidth {

  /**
   * Verify that onConfigurationChange updates the RateLimiter rate dynamically.
   */
  @Test
  public void testBandwidthDynamicUpdate() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hbase.replication.source.fs.conf.provider",
      TestSourceFSConfigurationProvider.class.getCanonicalName());
    ReplicationSink sink = new ReplicationSink(conf, null);

    // Default: unlimited
    assertEquals(Double.MAX_VALUE, readBulkLoadCopyRateLimiterRate(sink), 0.0);

    // Apply 50 MB/s
    Configuration newConf = new Configuration(conf);
    newConf.setDouble(HFileReplicator.REPLICATION_BULKLOAD_COPY_BANDWIDTH_MB_KEY, 50.0);
    sink.onConfigurationChange(newConf);
    assertEquals(50.0 * 1024 * 1024, readBulkLoadCopyRateLimiterRate(sink), 1.0);

    // Reset to unlimited (0 = no limit)
    Configuration resetConf = new Configuration(conf);
    resetConf.setDouble(HFileReplicator.REPLICATION_BULKLOAD_COPY_BANDWIDTH_MB_KEY, 0);
    sink.onConfigurationChange(resetConf);
    assertEquals(Double.MAX_VALUE, readBulkLoadCopyRateLimiterRate(sink), 0.0);
  }

  private double readBulkLoadCopyRateLimiterRate(ReplicationSink sink) throws Exception {
    Field field = ReplicationSink.class.getDeclaredField("bulkLoadCopyRateLimiter");
    field.setAccessible(true);
    return ((RateLimiter) field.get(sink)).getRate();
  }
}
