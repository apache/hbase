/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableMultiplexer.FlushWorker;
import org.apache.hadoop.hbase.client.HTableMultiplexer.PutStatus;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(SmallTests.class)
public class TestHTableMultiplexerViaMocks {

  private static final int NUM_RETRIES = HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER;
  private HTableMultiplexer mockMultiplexer;
  private ClusterConnection mockConnection;
  private HRegionLocation mockRegionLocation;
  private HRegionInfo mockRegionInfo;

  private TableName tableName;
  private Put put;

  @Before
  public void setupTest() {
    mockMultiplexer = mock(HTableMultiplexer.class);
    mockConnection = mock(ClusterConnection.class);
    mockRegionLocation = mock(HRegionLocation.class);
    mockRegionInfo = mock(HRegionInfo.class);

    tableName = TableName.valueOf("my_table");
    put = new Put(getBytes("row1"));
    put.addColumn(getBytes("f1"), getBytes("q1"), getBytes("v11"));
    put.addColumn(getBytes("f1"), getBytes("q2"), getBytes("v12"));
    put.addColumn(getBytes("f2"), getBytes("q1"), getBytes("v21"));

    // Call the real put(TableName, Put, int) method
    when(mockMultiplexer.put(any(TableName.class), any(Put.class), anyInt())).thenCallRealMethod();

    // Return the mocked ClusterConnection
    when(mockMultiplexer.getConnection()).thenReturn(mockConnection);

    // Return the regionInfo from the region location
    when(mockRegionLocation.getRegionInfo()).thenReturn(mockRegionInfo);

    // Make sure this RegionInfo points to our table
    when(mockRegionInfo.getTable()).thenReturn(tableName);
  }

  @Test public void useCacheOnInitialPut() throws Exception {
    mockMultiplexer.put(tableName, put, NUM_RETRIES);

    verify(mockMultiplexer)._put(tableName, put, NUM_RETRIES, false);
  }

  @Test public void nonNullLocationQueuesPut() throws Exception {
    final LinkedBlockingQueue<PutStatus> queue = new LinkedBlockingQueue<>();

    // Call the real method for _put(TableName, Put, int, boolean)
    when(mockMultiplexer._put(any(TableName.class), any(Put.class), anyInt(), anyBoolean())).thenCallRealMethod();

    // Return a region location
    when(mockConnection.getRegionLocation(tableName, put.getRow(), false)).thenReturn(mockRegionLocation);
    when(mockMultiplexer.getQueue(mockRegionLocation)).thenReturn(queue);

    assertTrue("Put should have been queued", mockMultiplexer.put(tableName, put, NUM_RETRIES));

    assertEquals(1, queue.size());
    final PutStatus ps = queue.take();
    assertEquals(put, ps.put);
    assertEquals(mockRegionInfo, ps.regionInfo);
  }

  @Test public void ignoreCacheOnRetriedPut() throws Exception {
    FlushWorker mockFlushWorker = mock(FlushWorker.class);
    ScheduledExecutorService mockExecutor = mock(ScheduledExecutorService.class);
    final AtomicInteger retryInQueue = new AtomicInteger(0);
    final AtomicLong totalFailedPuts = new AtomicLong(0L);
    final int maxRetryInQueue = 20;
    final long delay = 100L;

    final PutStatus ps = new PutStatus(mockRegionInfo, put, NUM_RETRIES);

    // Call the real resubmitFailedPut(PutStatus, HRegionLocation) method
    when(mockFlushWorker.resubmitFailedPut(any(PutStatus.class), any(HRegionLocation.class))).thenCallRealMethod();
    // Succeed on the re-submit without caching
    when(mockMultiplexer._put(tableName, put, NUM_RETRIES - 1, true)).thenReturn(true);

    // Stub out the getters for resubmitFailedPut(PutStatus, HRegionLocation)
    when(mockFlushWorker.getExecutor()).thenReturn(mockExecutor);
    when(mockFlushWorker.getNextDelay(anyInt())).thenReturn(delay);
    when(mockFlushWorker.getMultiplexer()).thenReturn(mockMultiplexer);
    when(mockFlushWorker.getRetryInQueue()).thenReturn(retryInQueue);
    when(mockFlushWorker.getMaxRetryInQueue()).thenReturn(maxRetryInQueue);
    when(mockFlushWorker.getTotalFailedPutCount()).thenReturn(totalFailedPuts);

    // When a Runnable is scheduled, run that Runnable
    when(mockExecutor.schedule(any(Runnable.class), eq(delay), eq(TimeUnit.MILLISECONDS))).thenAnswer(
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            // Before we run this, should have one retry in progress.
            assertEquals(1L, retryInQueue.get());

            Object[] args = invocation.getArguments();
            assertEquals(3, args.length);
            assertTrue("Argument should be an instance of Runnable", args[0] instanceof Runnable);
            Runnable runnable = (Runnable) args[0];
            runnable.run();
            return null;
          }
        });

    // The put should be rescheduled
    assertTrue("Put should have been rescheduled", mockFlushWorker.resubmitFailedPut(ps, mockRegionLocation));

    verify(mockMultiplexer)._put(tableName, put, NUM_RETRIES - 1, true);
    assertEquals(0L, totalFailedPuts.get());
    // Net result should be zero (added one before rerunning, subtracted one after running).
    assertEquals(0L, retryInQueue.get());
  }

  @SuppressWarnings("deprecation")
  @Test public void testConnectionClosing() throws IOException {
    doCallRealMethod().when(mockMultiplexer).close();
    // If the connection is not closed
    when(mockConnection.isClosed()).thenReturn(false);

    mockMultiplexer.close();

    // We should close it
    verify(mockConnection).close();
  }

  @SuppressWarnings("deprecation")
  @Test public void testClosingAlreadyClosedConnection() throws IOException {
    doCallRealMethod().when(mockMultiplexer).close();
    // If the connection is already closed
    when(mockConnection.isClosed()).thenReturn(true);

    mockMultiplexer.close();

    // We should not close it again
    verify(mockConnection, times(0)).close();
  }

  /**
   * @return UTF-8 byte representation for {@code str}
   */
  private static byte[] getBytes(String str) {
    return str.getBytes(UTF_8);
  }
}
