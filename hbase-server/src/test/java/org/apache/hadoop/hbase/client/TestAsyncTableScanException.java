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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableScanException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableScanException.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("scan");

  private static byte[] FAMILY = Bytes.toBytes("family");

  private static byte[] QUAL = Bytes.toBytes("qual");

  private static AsyncConnection CONN;

  private static AtomicInteger REQ_COUNT = new AtomicInteger();

  private static volatile int ERROR_AT;

  private static volatile boolean ERROR;

  private static volatile boolean DO_NOT_RETRY;

  private static final int ROW_COUNT = 100;

  public static final class ErrorCP implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> c,
        InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
      REQ_COUNT.incrementAndGet();
      if ((ERROR_AT == REQ_COUNT.get()) || ERROR) {
        if (DO_NOT_RETRY) {
          throw new DoNotRetryIOException("Injected exception");
        } else {
          throw new IOException("Injected exception");
        }
      }
      return RegionObserver.super.postScannerNext(c, s, result, limit, hasNext);
    }

  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
    UTIL.getAdmin()
      .createTable(TableDescriptorBuilder.newBuilder(TABLE_NAME)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setCoprocessor(ErrorCP.class.getName()).build());
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      for (int i = 0; i < ROW_COUNT; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUAL, Bytes.toBytes(i)));
      }
    }
    CONN = ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUpBeforeTest() {
    REQ_COUNT.set(0);
    ERROR_AT = 0;
    ERROR = false;
    DO_NOT_RETRY = false;
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testDoNotRetryIOException() throws IOException {
    ERROR_AT = 1;
    DO_NOT_RETRY = true;
    try (ResultScanner scanner = CONN.getTable(TABLE_NAME).getScanner(FAMILY)) {
      scanner.next();
    }
  }

  @Test
  public void testIOException() throws IOException {
    ERROR = true;
    try (ResultScanner scanner =
      CONN.getTableBuilder(TABLE_NAME).setMaxAttempts(3).build().getScanner(FAMILY)) {
      scanner.next();
      fail();
    } catch (RetriesExhaustedException e) {
      // expected
      assertThat(e.getCause(), instanceOf(ScannerResetException.class));
    }
    assertTrue(REQ_COUNT.get() >= 3);
  }

  private void count() throws IOException {
    try (ResultScanner scanner = CONN.getTable(TABLE_NAME).getScanner(new Scan().setCaching(1))) {
      for (int i = 0; i < ROW_COUNT; i++) {
        Result result = scanner.next();
        assertArrayEquals(Bytes.toBytes(i), result.getRow());
        assertArrayEquals(Bytes.toBytes(i), result.getValue(FAMILY, QUAL));
      }
    }
  }

  @Test
  public void testRecoveryFromScannerResetWhileOpening() throws IOException {
    ERROR_AT = 1;
    count();
    // we should at least request 1 time otherwise the error will not be triggered, and then we
    // need at least one more request to get the remaining results.
    assertTrue(REQ_COUNT.get() >= 2);
  }

  @Test
  public void testRecoveryFromScannerResetInTheMiddle() throws IOException {
    ERROR_AT = 2;
    count();
    // we should at least request 2 times otherwise the error will not be triggered, and then we
    // need at least one more request to get the remaining results.
    assertTrue(REQ_COUNT.get() >= 3);
  }
}
