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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.regionserver.RequestStatsCollector.COLLECTION_SIZE_PERCENT_MAX;
import static org.apache.hadoop.hbase.regionserver.RequestStatsCollector.COLLECTION_SIZE_PERCENT_MIN;
import static org.apache.hadoop.hbase.regionserver.RequestStatsCollector.SAMPLING_RATE_MAX;
import static org.apache.hadoop.hbase.regionserver.RequestStatsCollector.SAMPLING_RATE_MIN;
import static org.apache.hadoop.hbase.regionserver.RequestStatsCollector.TOP_N_MAX;
import static org.apache.hadoop.hbase.regionserver.RequestStatsCollector.TOP_N_MIN;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.RequestStatsCollector.OrderBy;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRequestStatsCollector {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRequestStatsCollector.class);

  @Test(expected = IllegalArgumentException.class)
  public void testMinRatioOfTotalHeap() {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.start(COLLECTION_SIZE_PERCENT_MIN - 1, 100);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxRatioOfTotalHeap() {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.start(COLLECTION_SIZE_PERCENT_MAX + 10, 100);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMinSamplingRate() {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.start(1, SAMPLING_RATE_MIN - 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxSamplingRate() {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.start(1, SAMPLING_RATE_MAX + 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMinBytes() {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.startInternal(0, 100);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaxBytes() {
    RequestStatsCollector collector = new RequestStatsCollector();
    long maxMemory =
      (long) (Runtime.getRuntime().maxMemory() * (COLLECTION_SIZE_PERCENT_MAX + 0.1f));
    collector.startInternal(maxMemory, 100);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryMinTopN() {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.start(1, 100);
    collector.query(RequestStat::countsAsDouble, TOP_N_MIN - 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryMaxTopN() {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.start(1, 100);
    collector.query(RequestStat::countsAsDouble, TOP_N_MAX + 1);
  }

  @Test
  public void testSize() {
    RequestStatsCollector collector = new RequestStatsCollector();

    // Not started yet
    assertEquals(-1, collector.getSize());
    assertEquals(-1, collector.getMaxSize());

    // Verify specified size
    collector.startInternal(2 * 1024 * 1024, 100);
    assertEquals(0, collector.getSize());
    assertEquals(2 * 1024 * 1024, collector.getMaxSize());
  }

  @Test
  public void testStartStop() throws InterruptedException {
    Request request;
    RequestStat stat;

    RequestStatsCollector collector = new RequestStatsCollector();

    // Verify start/stop time
    assertEquals(0, collector.getStartTime());
    assertEquals(0, collector.getStopTime());

    // Start the collector
    collector.start(1, 100);
    assertTrue(collector.getStartTime() > 0);
    assertEquals(0, collector.getStopTime());

    request = new Request("row1".getBytes(), "region1".getBytes(), Operation.GET, "client:1000");
    stat = collector.get(request);
    assertNull(stat);

    collector.add("row1".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 1, 300, 200, 100);
    stat = collector.get(request);
    // collector.getSize() is updated asynchronously
    Thread.sleep(20);
    assertEquals(1, stat.counts());
    assertEquals(300, stat.requestSizeSumBytes());
    assertEquals(200, stat.responseTimeSumMs());
    assertEquals(100, stat.responseSizeSumBytes());
    assertEquals(80, request.heapSize());
    assertEquals(112, collector.getSize());
    assertEquals(1, collector.getEntryCount());

    collector.add("row1".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 1, 300, 200, 100);
    stat = collector.get(request);
    Thread.sleep(20);
    assertEquals(2, stat.counts());
    assertEquals(600, stat.requestSizeSumBytes());
    assertEquals(400, stat.responseTimeSumMs());
    assertEquals(200, stat.responseSizeSumBytes());
    assertEquals(80, request.heapSize());
    assertEquals(112, collector.getSize());
    assertEquals(1, collector.getEntryCount());

    // Verify no changes after stop
    collector.stop();
    assertTrue(collector.getStartTime() > 0);
    assertTrue(collector.getStopTime() > 0);
    collector.add("row1".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 1, 300, 200, 100);
    stat = collector.get(request);
    Thread.sleep(20);
    assertEquals(2, stat.counts());
    assertEquals(600, stat.requestSizeSumBytes());
    assertEquals(400, stat.responseTimeSumMs());
    assertEquals(200, stat.responseSizeSumBytes());
    assertEquals(80, request.heapSize());
    assertEquals(112, collector.getSize());
    assertEquals(1, collector.getEntryCount());

    // Verify clear after start
    collector.start(1, 100);
    stat = collector.get(request);
    Thread.sleep(20);
    assertNull(stat);
    assertEquals(0, collector.getSize());
    assertEquals(0, collector.getEntryCount());

    // Verify clear after stop
    collector.stop();
    collector.clear();
    Thread.sleep(20);
    assertEquals(0, collector.getStartTime());
    assertEquals(0, collector.getStopTime());
  }

  @Test(expected = IllegalStateException.class)
  public void testClear() {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.start(1, 100);
    // Can be cleared only after stop
    collector.clear();
  }

  @Test
  public void testRequestEqualsAndHashCode() {
    Request requestA =
      new Request("row1".getBytes(), "region1".getBytes(), Operation.GET, "client:1000");
    Request requestB =
      new Request("row1".getBytes(), "region1".getBytes(), Operation.GET, "client:1000");
    assertEquals(requestA, requestB);
    assertEquals(requestA.hashCode(), requestB.hashCode());
    assertNotSame(requestA, requestB);
  }

  @Test
  public void testRequestHeapSize() {
    Request request;
    request = new Request("row1".getBytes(), "region1".getBytes(), Operation.GET, "client:1000");
    assertEquals(80, request.heapSize());
    request = new Request("row1".getBytes(), "region1234".getBytes(), Operation.GET, "client:1000");
    assertEquals(88, request.heapSize());
    request =
      new Request("row123456".getBytes(), "region1234".getBytes(), Operation.GET, "client:1000");
    assertEquals(96, request.heapSize());
  }

  @Test
  public void testRequestStatHeapSize() {
    RequestStat stat;
    stat = new RequestStat();
    stat.add(1, 300, 100, 200);
    assertEquals(32, stat.heapSize());
    stat.add(1000000000000000L, 1000000000000000L, 10000000000000L, 200000000000L);
    assertEquals(32, stat.heapSize());
  }

  @Test
  public void testEviction() throws InterruptedException {
    Request request =
      new Request("row1".getBytes(), "region1".getBytes(), Operation.GET, "client:1000");
    RequestStat stat = new RequestStat();
    assertEquals(80, request.heapSize());
    assertEquals(32, stat.heapSize());

    // Set smaller than the size of request heap size plus stat heap size.
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.startInternal(request.heapSize() + stat.heapSize() - 1, 100);

    collector.add("row1".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 1, 300, 200, 100);
    RequestStat retrievedStat1 = collector.get(request);
    // It will be evicted after next put, but it is not evicted yet.
    assertEquals(0, collector.getSize());
    assertEquals(1, retrievedStat1.counts());
    assertEquals(300, retrievedStat1.requestSizeSumBytes());
    assertEquals(200, retrievedStat1.responseTimeSumMs());
    assertEquals(100, retrievedStat1.responseSizeSumBytes());

    // Put again, it will be evicted.
    Thread.sleep(20);
    collector.add("row1".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 1, 300, 200, 100);
    retrievedStat1 = collector.get(request);
    // The stat of the same request is not merged, because the previous one is evicted.
    assertEquals(0, collector.getSize());
    assertEquals(1, retrievedStat1.counts());
    assertEquals(300, retrievedStat1.requestSizeSumBytes());
    assertEquals(200, retrievedStat1.responseTimeSumMs());
    assertEquals(100, retrievedStat1.responseSizeSumBytes());
  }

  @Test
  public void testQuery() {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.start(1, 100);

    List<Map.Entry<Request, RequestStat>> result;
    int topN = 3;

    // No request yet
    result = collector.query(RequestStat::countsAsDouble, topN);
    assertEquals(0, result.size());

    // Do requests
    for (int i = 0; i < 10; i++) {
      collector.add("row1".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
        Operation.GET, "client:1000", 1000, 100, 10, 1);
    }
    for (int i = 0; i < 10; i++) {
      collector.add("row2".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
        Operation.GET, "client:1000", 100, 10, 1, 1000);
    }
    for (int i = 0; i < 10; i++) {
      collector.add("row3".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
        Operation.GET, "client:1000", 10, 1, 1000, 100);
    }
    for (int i = 0; i < 10; i++) {
      collector.add("row4".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
        Operation.GET, "client:1000", 1, 1000, 100, 10);
    }
    assertEquals(4, collector.getEntryCount());

    // Summarize by count descending
    result = collector.query(RequestStat::countsAsDouble, topN);
    assertEquals(3, result.size());
    assertArrayEquals("row1".getBytes(), result.get(0).getKey().rowKey());
    assertEquals(10000, result.get(0).getValue().counts());
    assertArrayEquals("row2".getBytes(), result.get(1).getKey().rowKey());
    assertEquals(1000, result.get(1).getValue().counts());
    assertArrayEquals("row3".getBytes(), result.get(2).getKey().rowKey());
    assertEquals(100, result.get(2).getValue().counts());

    // Summarize by responseSize descending
    result = collector.query(RequestStat::requestSizeSumBytesAsDouble, topN);
    assertEquals(3, result.size());
    assertArrayEquals("row4".getBytes(), result.get(0).getKey().rowKey());
    assertEquals(10000, result.get(0).getValue().requestSizeSumBytes());
    assertArrayEquals("row1".getBytes(), result.get(1).getKey().rowKey());
    assertEquals(1000, result.get(1).getValue().requestSizeSumBytes());
    assertArrayEquals("row2".getBytes(), result.get(2).getKey().rowKey());
    assertEquals(100, result.get(2).getValue().requestSizeSumBytes());

    // Summarize by responseSize descending
    result = collector.query(RequestStat::responseSizeSumBytesAsDouble, topN);
    assertEquals(3, result.size());
    assertArrayEquals("row2".getBytes(), result.get(0).getKey().rowKey());
    assertEquals(10000, result.get(0).getValue().responseSizeSumBytes());
    assertArrayEquals("row3".getBytes(), result.get(1).getKey().rowKey());
    assertEquals(1000, result.get(1).getValue().responseSizeSumBytes());
    assertArrayEquals("row4".getBytes(), result.get(2).getKey().rowKey());
    assertEquals(100, result.get(2).getValue().responseSizeSumBytes());

    // Summarize by responseTime descending
    result = collector.query(RequestStat::responseTimeSumMsAsDouble, topN);
    assertEquals(3, result.size());
    assertArrayEquals("row3".getBytes(), result.get(0).getKey().rowKey());
    assertEquals(10000, result.get(0).getValue().responseTimeSumMs());
    assertArrayEquals("row4".getBytes(), result.get(1).getKey().rowKey());
    assertEquals(1000, result.get(1).getValue().responseTimeSumMs());
    assertArrayEquals("row1".getBytes(), result.get(2).getKey().rowKey());
    assertEquals(100, result.get(2).getValue().responseTimeSumMs());
  }

  @Test
  public void testQueryTieBreaker() {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.start(1, 100);

    List<Map.Entry<Request, RequestStat>> result;
    int topN = 4;

    collector.add("row1".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 2, 1, 1, 1);
    collector.add("row2".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 1, 2, 1, 1);
    collector.add("row3".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 1, 1, 1, 1);
    collector.add("row4".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 1, 1, 1, 2);
    collector.add("row5".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 1, 1, 2, 1);

    result = collector.query(RequestStat::countsAsDouble, topN);
    assertEquals(4, result.size());
    assertEquals("row1", Bytes.toStringBinary(result.get(0).getKey().rowKey()));
    assertEquals("row5", Bytes.toStringBinary(result.get(1).getKey().rowKey()));
    assertEquals("row4", Bytes.toStringBinary(result.get(2).getKey().rowKey()));
    assertEquals("row2", Bytes.toStringBinary(result.get(3).getKey().rowKey()));
  }

  @Test
  public void testRowRequest() {
    Request requestA, requestB;

    // rowKey is null
    requestA = new Request(null, "region1".getBytes(), Operation.SCAN, "client:1000");
    requestB = new Request(null, "region1".getBytes(), Operation.SCAN, "client:1000");
    assertEquals(72, requestA.heapSize());
    assertEquals(72, requestB.heapSize());
    assertEquals(requestA.hashCode(), requestB.hashCode());
    assertEquals(requestA, requestB);
    assertNotSame(requestA, requestB);

    // region is null
    requestA = new Request("row".getBytes(), null, Operation.SCAN, "client:1000");
    requestB = new Request("row".getBytes(), null, Operation.SCAN, "client:1000");
    assertEquals(72, requestA.heapSize());
    assertEquals(72, requestB.heapSize());
    assertEquals(requestA, requestB);
    assertNotSame(requestA, requestB);
  }

  @Test(expected = IllegalStateException.class)
  public void testClearWithoutStop() {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.start(1, 100);
    collector.clear();
  }

  @Test
  public void testQueryWithoutStart() {
    RequestStatsCollector collector = new RequestStatsCollector();

    // Not started yet
    List<Map.Entry<Request, RequestStat>> result = collector.query(RequestStat::countsAsDouble, 3);
    assertEquals(0, result.size());
  }

  @Test
  public void testQueryAndBuildResultJson() throws InterruptedException {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.start(1, 100);

    String json;

    // empty report
    json = collector.queryAndBuildResultJson(2, OrderBy.COUNTS);
    assertEquals(ignoreTime(
      """
          {"info":{"status":"RUNNING","startTime":0,"stopTime":0,"maxSize":"22 MB","size":"0 B","entryCount":0},"stats":[],"executionTimeMs":0,"isSql":false}"""),
      ignoreTime(json));

    // Do requests
    collector.add("row1".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 1, 1, 1, 1);
    collector.add("row2".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 2, 2, 2, 2);
    collector.add(Bytes.toBytes(1L), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 3, 3, 3, 3);
    Thread.sleep(20);
    json = collector.queryAndBuildResultJson(2, OrderBy.COUNTS);
    assertEquals(ignoreTime(
      """
          {"info":{"status":"RUNNING","startTime":0,"stopTime":0,"maxSize":"22 MB","size":"336 B","entryCount":3},"stats":[{"rowKey":"\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\\\\x01","region":"region1","table":"table1","operation":"GET","client":"client:1000","counts":3,"requestSizeSumBytes":3,"requestSizeAvgBytes":1.0,"responseTimeSumMs":3,"responseTimeAvgMs":1.0,"responseSizeSumBytes":3,"responseSizeAvgBytes":1.0},{"rowKey":"row2","region":"region1","table":"table1","operation":"GET","client":"client:1000","counts":2,"requestSizeSumBytes":2,"requestSizeAvgBytes":1.0,"responseTimeSumMs":2,"responseTimeAvgMs":1.0,"responseSizeSumBytes":2,"responseSizeAvgBytes":1.0}],"executionTimeMs":0,"isSql":false}"""),
      ignoreTime(json));
  }

  private static String ignoreTime(String status) {
    return status.replaceAll("\"startTime\":[0-9]+", "\"startTime\":0")
      .replaceAll("\"stopTime\":[0-9]+", "\"stopTime\":0")
      .replaceAll("\"executionTimeMs\":[0-9]+", "\"executionTimeMs\":0");
  }

  @Test
  public void testStatusJson() throws InterruptedException {
    RequestStatsCollector collector = new RequestStatsCollector();

    // Not started yet
    String json = collector.buildInfoJson();
    assertEquals(
      """
          {"status":"STOPPED","startTime":0,"stopTime":0,"maxSize":"N/A","size":"N/A","entryCount":-1}""",
      json);

    collector.startInternal(1024, 100);
    json = collector.buildInfoJson();
    assertEquals(ignoreTime(
      """
          {"status":"RUNNING","startTime":0,"stopTime":0,"maxSize":"1 KB","size":"0 B","entryCount":0}"""),
      ignoreTime(json));

    collector.add("row1".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 1, 1, 1, 1);
    Thread.sleep(20);
    json = collector.buildInfoJson();
    assertEquals(ignoreTime(
      """
          {"status":"RUNNING","startTime":0,"stopTime":0,"maxSize":"1 KB","size":"112 B","entryCount":1}"""),
      ignoreTime(json));

    collector.stop();
    Thread.sleep(20);
    json = collector.buildInfoJson();
    assertEquals(ignoreTime(
      """
          {"status":"STOPPED","startTime":0,"stopTime":0,"maxSize":"1 KB","size":"112 B","entryCount":1}"""),
      ignoreTime(json));

    collector.clear();
    json = collector.buildInfoJson();
    assertEquals(ignoreTime(
      """
          {"status":"STOPPED","startTime":0,"stopTime":0,"maxSize":"N/A","size":"N/A","entryCount":-1}"""),
      ignoreTime(json));
  }

  @Test
  public void testSamplingRate() {
    RequestStatsCollector collector = new RequestStatsCollector();

    // Sampling 1%
    collector.start(1, 10);
    for (int i = 0; i < 1000; i++) {
      collector.add(Bytes.toBytes(i), "region1".getBytes(), TableName.valueOf("table1"),
        Operation.GET, "client:1000", 1, 1, 1, 1);
    }
    // About 100 entries are sampled
    assert (collector.getEntryCount() > 10 && collector.getEntryCount() < 200);
  }

  @Test(expected = IllegalStateException.class)
  public void testConcurrentQuery() {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.setQueryInProgress(false);
    collector.start(1, 100);
    collector.setQueryInProgress(true);
    collector.query(RequestStat::countsAsDouble, 10);
  }

  @Test(expected = IllegalStateException.class)
  public void testConcurrentSql() throws SQLException, ClassNotFoundException {
    RequestStatsCollector collector = new RequestStatsCollector();
    collector.setQueryInProgress(false);
    collector.start(1, 100);
    collector.setQueryInProgress(true);
    collector.sql("empty", 10);
  }

  @Test
  public void testSQL() throws SQLException, ClassNotFoundException, InterruptedException {
    RequestStatsCollector collector = new RequestStatsCollector();

    String json;

    // Not started yet
    json = collector.sql("""
        select *
        from stats
        where operation = 'GET'
        order by responseTimeSum desc
        """, 2);
    assertEquals(ignoreTime(
      """
          {"info":{"status":"STOPPED","startTime":0,"stopTime":0,"maxSize":"N/A","size":"N/A","entryCount":-1},"stats":[],"executionTimeMs":0,"isSql":true}"""),
      ignoreTime(json));

    // Do requests
    collector.start(1, 100);
    collector.add("row1".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 1, 1, 1, 1);
    collector.add("row2".getBytes(), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 2, 2, 2, 2);
    collector.add(Bytes.toBytes(1L), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.GET, "client:1000", 3, 3, 3, 3);
    collector.add(Bytes.toBytes(1L), "region1".getBytes(), TableName.valueOf("table1"),
      Operation.PUT, "client:1000", 3, 4, 4, 4);
    Thread.sleep(20);

    json = collector.sql("""
        select *
        from stats
        where operation = 'GET'
        order by responseTimeSumMs desc
        """, 2);
    assertEquals(ignoreTime(
      """
          {"info":{"status":"RUNNING","startTime":0,"stopTime":0,"maxSize":"22 MB","size":"448 B","entryCount":4},"stats":[{"rowKey":"\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\\\\x00\\\\x01","region":"region1","table":"table1","operation":"GET","client":"client:1000","counts":3,"requestSizeSumBytes":3,"responseTimeSumMs":3,"responseSizeSumBytes":3},{"rowKey":"row2","region":"region1","table":"table1","operation":"GET","client":"client:1000","counts":2,"requestSizeSumBytes":2,"responseTimeSumMs":2,"responseSizeSumBytes":2}],"executionTimeMs":0,"isSql":true}"""),
      ignoreTime(json));
  }
}
