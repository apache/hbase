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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.backoff.ExponentialClientBackoffPolicy;
import org.apache.hadoop.hbase.client.backoff.ServerStatistics;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestClientExponentialBackoff {

  ServerName server = Mockito.mock(ServerName.class);
  byte[] regionname = Bytes.toBytes("region");

  @Test
  public void testNulls() {
    Configuration conf = new Configuration(false);
    ExponentialClientBackoffPolicy backoff = new ExponentialClientBackoffPolicy(conf);
    assertEquals(0, backoff.getBackoffTime(null, null, null));

    // server name doesn't matter to calculation, but check it now anyways
    assertEquals(0, backoff.getBackoffTime(server, null, null));
    assertEquals(0, backoff.getBackoffTime(server, regionname, null));

    // check when no stats for the region yet
    ServerStatistics stats = new ServerStatistics();
    assertEquals(0, backoff.getBackoffTime(server, regionname, stats));
  }

  @Test
  public void testMaxLoad() {
    Configuration conf = new Configuration(false);
    ExponentialClientBackoffPolicy backoff = new ExponentialClientBackoffPolicy(conf);

    ServerStatistics stats = new ServerStatistics();
    update(stats, 100);
    assertEquals(ExponentialClientBackoffPolicy.DEFAULT_MAX_BACKOFF, backoff.getBackoffTime(server,
        regionname, stats));

    // another policy with a different max timeout
    long max = 100;
    conf.setLong(ExponentialClientBackoffPolicy.MAX_BACKOFF_KEY, max);
    ExponentialClientBackoffPolicy backoffShortTimeout = new ExponentialClientBackoffPolicy(conf);
    assertEquals(max, backoffShortTimeout.getBackoffTime(server, regionname, stats));

    // test beyond 100 still doesn't exceed the max
    update(stats, 101);
    assertEquals(ExponentialClientBackoffPolicy.DEFAULT_MAX_BACKOFF, backoff.getBackoffTime(server,
        regionname, stats));
    assertEquals(max, backoffShortTimeout.getBackoffTime(server, regionname, stats));

    // and that when we are below 100, its less than the max timeout
    update(stats, 99);
    assertTrue(backoff.getBackoffTime(server,
        regionname, stats) < ExponentialClientBackoffPolicy.DEFAULT_MAX_BACKOFF);
    assertTrue(backoffShortTimeout.getBackoffTime(server, regionname, stats) < max);
  }

  /**
   * Make sure that we get results in the order that we expect - backoff for a load of 1 should
   * less than backoff for 10, which should be less than that for 50.
   */
  @Test
  public void testResultOrdering() {
    Configuration conf = new Configuration(false);
    // make the max timeout really high so we get differentiation between load factors
    conf.setLong(ExponentialClientBackoffPolicy.MAX_BACKOFF_KEY, Integer.MAX_VALUE);
    ExponentialClientBackoffPolicy backoff = new ExponentialClientBackoffPolicy(conf);

    ServerStatistics stats = new ServerStatistics();
    long previous = backoff.getBackoffTime(server, regionname, stats);
    for (int i = 1; i <= 100; i++) {
      update(stats, i);
      long next = backoff.getBackoffTime(server, regionname, stats);
      assertTrue(
          "Previous backoff time" + previous + " >= " + next + ", the next backoff time for " +
              "load " + i, previous < next);
      previous = next;
    }
  }

  @Test
  public void testHeapOccupancyPolicy() {
    Configuration conf = new Configuration(false);
    ExponentialClientBackoffPolicy backoff = new ExponentialClientBackoffPolicy(conf);

    ServerStatistics stats = new ServerStatistics();
    long backoffTime;

    update(stats, 0, 95);
    backoffTime = backoff.getBackoffTime(server, regionname, stats);
    assertTrue("Heap occupancy at low watermark had no effect", backoffTime > 0);

    long previous = backoffTime;
    update(stats, 0, 96);
    backoffTime = backoff.getBackoffTime(server, regionname, stats);
    assertTrue("Increase above low watermark should have increased backoff",
      backoffTime > previous);

    update(stats, 0, 98);
    backoffTime = backoff.getBackoffTime(server, regionname, stats);
    assertEquals("We should be using max backoff when at high watermark", backoffTime,
      ExponentialClientBackoffPolicy.DEFAULT_MAX_BACKOFF);
  }

  private void update(ServerStatistics stats, int load) {
    ClientProtos.RegionLoadStats stat = ClientProtos.RegionLoadStats.newBuilder()
        .setMemstoreLoad
            (load).build();
    stats.update(regionname, stat);
  }

  private void update(ServerStatistics stats, int memstoreLoad, int heapOccupancy) {
    ClientProtos.RegionLoadStats stat = ClientProtos.RegionLoadStats.newBuilder()
        .setMemstoreLoad(memstoreLoad)
        .setHeapOccupancy(heapOccupancy)
            .build();
    stats.update(regionname, stat);
  }
}
