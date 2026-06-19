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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Unit tests for bulkload copy bandwidth throttling in {@link HFileReplicator} and
 * {@link ReplicationSink}. Does not require a running HBase cluster.
 */
@Tag(ReplicationTests.TAG)
@Tag(SmallTests.TAG)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestHFileReplicatorBandwidth {

  private final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @BeforeAll
  public void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniDFSCluster(1);
  }

  @AfterAll
  public void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  /**
   * Verify that onConfigurationChange updates the RateLimiter rate dynamically.
   */
  @Test
  public void testBandwidthDynamicUpdate() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set("hbase.replication.source.fs.conf.provider",
      TestSourceFSConfigurationProvider.class.getCanonicalName());
    ReplicationSink sink = new ReplicationSink(conf, null);

    // Default: unlimited
    assertEquals(Double.MAX_VALUE, sink.getBulkLoadCopyRateLimiterRate(), 0.0);

    // Apply 50 MB/s
    Configuration newConf = new Configuration(conf);
    newConf.setDouble(HFileReplicator.REPLICATION_BULKLOAD_COPY_BANDWIDTH_MB_KEY, 50.0);
    sink.onConfigurationChange(newConf);
    assertEquals(50.0 * 1024 * 1024, sink.getBulkLoadCopyRateLimiterRate(), 1.0);

    // Reset to unlimited (0 = no limit)
    Configuration resetConf = new Configuration(conf);
    resetConf.setDouble(HFileReplicator.REPLICATION_BULKLOAD_COPY_BANDWIDTH_MB_KEY, 0);
    sink.onConfigurationChange(resetConf);
    assertEquals(Double.MAX_VALUE, sink.getBulkLoadCopyRateLimiterRate(), 0.0);
  }

  /**
   * Verify that throttled copy actually slows down I/O at the specified rate. Creates a file of
   * known size, copies it through a throttled stream at 1 MB/s, and asserts the elapsed time is at
   * least (fileSize / 1MB * 0.8) seconds.
   */
  @Test
  public void testCopyIsThrottled() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path testDir = TEST_UTIL.getDataTestDirOnTestFS("testCopyIsThrottled");
    fs.mkdirs(testDir);

    // Write a ~2MB test file
    Path srcFile = new Path(testDir, "src");
    int fileSize = 2 * 1024 * 1024;
    try (FSDataOutputStream out = fs.create(srcFile)) {
      writeBytes(out, fileSize);
    }

    Path dstFile = new Path(testDir, "dst");

    // Throttle at 1 MB/s
    double limitMbPerSec = 1.0;
    org.apache.hbase.thirdparty.com.google.common.util.concurrent.RateLimiter limiter =
      org.apache.hbase.thirdparty.com.google.common.util.concurrent.RateLimiter
        .create(limitMbPerSec * 1024 * 1024);

    long start = System.currentTimeMillis();
    try (java.io.InputStream in = fs.open(srcFile); OutputStream out = fs.create(dstFile)) {
      byte[] buf = new byte[65536];
      int bytesRead;
      while ((bytesRead = in.read(buf)) >= 0) {
        limiter.acquire(bytesRead);
        out.write(buf, 0, bytesRead);
      }
    }
    long elapsed = System.currentTimeMillis() - start;

    // At 1MB/s, 2MB should take >= 1600ms (allow 20% margin for JVM overhead)
    long minExpectedMs = (long) (fileSize * 1000L / (limitMbPerSec * 1024 * 1024) * 0.8);
    assertTrue(elapsed >= minExpectedMs,
      "Expected throttled copy >= " + minExpectedMs + "ms, got " + elapsed + "ms");
  }

  private static void writeBytes(OutputStream out, int size) throws IOException {
    byte[] buf = new byte[65536];
    int written = 0;
    while (written < size) {
      int chunk = Math.min(buf.length, size - written);
      out.write(buf, 0, chunk);
      written += chunk;
    }
  }
}
