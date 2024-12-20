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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer.RowStatisticsEventHandler;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer.RowStatisticsRingBufferEnvelope;
import org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer.RowStatisticsRingBufferPayload;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.impl.CounterImpl;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class TestRowStatisticsEventHandler {

  private static final String REGION_STRING = "REGION_STRING";
  private static final byte[] FULL_REGION = Bytes.toBytes("FULL_REGION_STRING");
  private static final String JSON_STRING = "JSON_STRING";
  private static final RowStatisticsRingBufferEnvelope EVENT =
    new RowStatisticsRingBufferEnvelope();
  private static final RowStatistics ROW_STATISTICS = mock(RowStatistics.class);
  private BufferedMutator bufferedMutator;
  private Counter failureCounter;
  private RowStatisticsEventHandler eventHandler;

  @Before
  public void setup() {
    bufferedMutator = mock(BufferedMutator.class);
    failureCounter = new CounterImpl();
    eventHandler = new RowStatisticsEventHandler(bufferedMutator, failureCounter);
    when(ROW_STATISTICS.getRegion()).thenReturn(REGION_STRING);
    when(ROW_STATISTICS.getJsonString()).thenReturn(JSON_STRING);
  }

  @Test
  public void itPersistsRowStatistics() throws Exception {
    EVENT.load(new RowStatisticsRingBufferPayload(ROW_STATISTICS, FULL_REGION));
    doNothing().when(bufferedMutator).mutate(any(Put.class));
    eventHandler.onEvent(EVENT, 0L, true);
    verify(bufferedMutator, times(1)).mutate(any(Put.class));
    assertEquals(failureCounter.getCount(), 0);
  }

  @Test
  public void itDoesNotPublishNullRowStatistics() throws Exception {
    EVENT.load(null);
    eventHandler.onEvent(EVENT, 0L, true);
    verify(bufferedMutator, times(0)).mutate(any(Put.class));
    assertEquals(failureCounter.getCount(), 0);
  }

  @Test
  public void itCountsFailedPersists() throws Exception {
    EVENT.load(new RowStatisticsRingBufferPayload(ROW_STATISTICS, FULL_REGION));
    doThrow(new IOException()).when(bufferedMutator).mutate(any(Put.class));
    eventHandler.onEvent(EVENT, 0L, true);
    verify(bufferedMutator, times(1)).mutate(any(Put.class));
    assertEquals(failureCounter.getCount(), 1);
  }
}
