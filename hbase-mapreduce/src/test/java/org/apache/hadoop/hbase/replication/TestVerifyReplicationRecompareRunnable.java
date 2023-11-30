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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplicationRecompareRunnable;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@Category({ ReplicationTests.class, SmallTests.class })
@RunWith(MockitoJUnitRunner.class)
public class TestVerifyReplicationRecompareRunnable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestVerifyReplicationRecompareRunnable.class);

  @Mock
  private Table sourceTable;

  @Mock
  private Table replicatedTable;

  @Mock
  private Mapper.Context context;

  static Result genResult(int cols) {
    KeyValue[] kvs = new KeyValue[cols];

    for (int i = 0; i < cols; ++i) {
      kvs[i] =
        new KeyValue(genBytes(), genBytes(), genBytes(), System.currentTimeMillis(), genBytes());
    }

    return Result.create(kvs);
  }

  static byte[] genBytes() {
    return Bytes.toBytes(ThreadLocalRandom.current().nextInt());
  }

  @Before
  public void setUp() {
    for (VerifyReplication.Verifier.Counters counter : VerifyReplication.Verifier.Counters
      .values()) {
      Counter emptyCounter = new GenericCounter(counter.name(), counter.name());
      when(context.getCounter(counter)).thenReturn(emptyCounter);
    }
  }

  @Test
  public void itRecomparesGoodRow() throws IOException {
    Result result = genResult(2);

    when(sourceTable.get(any(Get.class))).thenReturn(result);
    when(replicatedTable.get(any(Get.class))).thenReturn(result);

    VerifyReplicationRecompareRunnable runnable = new VerifyReplicationRecompareRunnable(context,
      genResult(5), null, VerifyReplication.Verifier.Counters.ONLY_IN_SOURCE_TABLE_ROWS, "",
      new Scan(), sourceTable, replicatedTable, 3, 1, 0, true);

    runnable.run();

    assertEquals(0, context.getCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
    assertEquals(0,
      context.getCounter(VerifyReplication.Verifier.Counters.ONLY_IN_SOURCE_TABLE_ROWS).getValue());
    assertEquals(1, context.getCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(1,
      context.getCounter(VerifyReplication.Verifier.Counters.SOURCE_ROW_CHANGED).getValue());
    assertEquals(1,
      context.getCounter(VerifyReplication.Verifier.Counters.PEER_ROW_CHANGED).getValue());
    assertEquals(2, context.getCounter(VerifyReplication.Verifier.Counters.RECOMPARES).getValue());
  }

  @Test
  public void itRecomparesBadRow() throws IOException {
    Result replicatedResult = genResult(1);
    when(sourceTable.get(any(Get.class))).thenReturn(genResult(5));
    when(replicatedTable.get(any(Get.class))).thenReturn(replicatedResult);

    VerifyReplicationRecompareRunnable runnable = new VerifyReplicationRecompareRunnable(context,
      genResult(5), replicatedResult, VerifyReplication.Verifier.Counters.ONLY_IN_SOURCE_TABLE_ROWS,
      "", new Scan(), sourceTable, replicatedTable, 1, 1, 0, true);

    runnable.run();

    assertEquals(1, context.getCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
    assertEquals(1,
      context.getCounter(VerifyReplication.Verifier.Counters.ONLY_IN_SOURCE_TABLE_ROWS).getValue());
    assertEquals(0, context.getCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(1,
      context.getCounter(VerifyReplication.Verifier.Counters.SOURCE_ROW_CHANGED).getValue());
    assertEquals(0,
      context.getCounter(VerifyReplication.Verifier.Counters.PEER_ROW_CHANGED).getValue());
    assertEquals(1, context.getCounter(VerifyReplication.Verifier.Counters.RECOMPARES).getValue());
  }

  @Test
  public void itHandlesExceptionOnRecompare() throws IOException {
    when(sourceTable.get(any(Get.class))).thenThrow(new IOException("Error!"));
    when(replicatedTable.get(any(Get.class))).thenReturn(genResult(5));

    VerifyReplicationRecompareRunnable runnable = new VerifyReplicationRecompareRunnable(context,
      genResult(5), null, VerifyReplication.Verifier.Counters.ONLY_IN_SOURCE_TABLE_ROWS, "",
      new Scan(), sourceTable, replicatedTable, 1, 1, 0, true);

    runnable.run();

    assertEquals(1, context.getCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
    assertEquals(1,
      context.getCounter(VerifyReplication.Verifier.Counters.ONLY_IN_SOURCE_TABLE_ROWS).getValue());
    assertEquals(1,
      context.getCounter(VerifyReplication.Verifier.Counters.FAILED_RECOMPARE).getValue());
    assertEquals(1, context.getCounter(VerifyReplication.Verifier.Counters.RECOMPARES).getValue());
  }
}
