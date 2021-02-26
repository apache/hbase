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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.regionserver.DelegatingKeyValueScanner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ReversedStoreScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MediumTests.class, ClientTests.class })
public class TestFromClientSideScanExcpetion {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFromClientSideScanExcpetion.class);

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static byte[] FAMILY = Bytes.toBytes("testFamily");

  private static int SLAVES = 3;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 6000000);
    conf.setClass(HConstants.REGION_IMPL, MyHRegion.class, HRegion.class);
    conf.setBoolean("hbase.client.log.scanner.activity", true);
    // We need more than one region server in this test
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static AtomicBoolean ON = new AtomicBoolean(false);
  private static AtomicLong REQ_COUNT = new AtomicLong(0);
  private static AtomicBoolean IS_DO_NOT_RETRY = new AtomicBoolean(false); // whether to throw
                                                                           // DNRIOE
  private static AtomicBoolean THROW_ONCE = new AtomicBoolean(true); // whether to only throw once

  private static void reset() {
    ON.set(false);
    REQ_COUNT.set(0);
    IS_DO_NOT_RETRY.set(false);
    THROW_ONCE.set(true);
  }

  private static void inject() {
    ON.set(true);
  }

  public static final class MyHRegion extends HRegion {

    @SuppressWarnings("deprecation")
    public MyHRegion(Path tableDir, WAL wal, FileSystem fs, Configuration confParam,
        RegionInfo regionInfo, TableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
    }

    @Override
    protected HStore instantiateHStore(ColumnFamilyDescriptor family, boolean warmup)
        throws IOException {
      return new MyHStore(this, family, conf, warmup);
    }
  }

  public static final class MyHStore extends HStore {

    public MyHStore(HRegion region, ColumnFamilyDescriptor family, Configuration confParam,
        boolean warmup) throws IOException {
      super(region, family, confParam, warmup);
    }

    @Override
    protected KeyValueScanner createScanner(Scan scan, ScanInfo scanInfo,
        NavigableSet<byte[]> targetCols, long readPt) throws IOException {
      return scan.isReversed() ? new ReversedStoreScanner(this, scanInfo, scan, targetCols, readPt)
          : new MyStoreScanner(this, scanInfo, scan, targetCols, readPt);
    }
  }

  public static final class MyStoreScanner extends StoreScanner {
    public MyStoreScanner(HStore store, ScanInfo scanInfo, Scan scan, NavigableSet<byte[]> columns,
        long readPt) throws IOException {
      super(store, scanInfo, scan, columns, readPt);
    }

    @Override
    protected List<KeyValueScanner> selectScannersFrom(HStore store,
        List<? extends KeyValueScanner> allScanners) {
      List<KeyValueScanner> scanners = super.selectScannersFrom(store, allScanners);
      List<KeyValueScanner> newScanners = new ArrayList<>(scanners.size());
      for (KeyValueScanner scanner : scanners) {
        newScanners.add(new DelegatingKeyValueScanner(scanner) {
          @Override
          public boolean reseek(Cell key) throws IOException {
            if (ON.get()) {
              REQ_COUNT.incrementAndGet();
              if (!THROW_ONCE.get() || REQ_COUNT.get() == 1) {
                if (IS_DO_NOT_RETRY.get()) {
                  throw new DoNotRetryIOException("Injected exception");
                } else {
                  throw new IOException("Injected exception");
                }
              }
            }
            return super.reseek(key);
          }
        });
      }
      return newScanners;
    }
  }

  /**
   * Tests the case where a Scan can throw an IOException in the middle of the seek / reseek leaving
   * the server side RegionScanner to be in dirty state. The client has to ensure that the
   * ClientScanner does not get an exception and also sees all the data.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testClientScannerIsResetWhenScanThrowsIOException()
      throws IOException, InterruptedException {
    reset();
    THROW_ONCE.set(true); // throw exceptions only once
    TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      int rowCount = TEST_UTIL.loadTable(t, FAMILY, false);
      TEST_UTIL.getAdmin().flush(tableName);
      inject();
      int actualRowCount = TEST_UTIL.countRows(t, new Scan().addColumn(FAMILY, FAMILY));
      assertEquals(rowCount, actualRowCount);
    }
    assertTrue(REQ_COUNT.get() > 0);
  }

  /**
   * Tests the case where a coprocessor throws a DoNotRetryIOException in the scan. The expectation
   * is that the exception will bubble up to the client scanner instead of being retried.
   */
  @Test
  public void testScannerThrowsExceptionWhenCoprocessorThrowsDNRIOE()
      throws IOException, InterruptedException {
    reset();
    IS_DO_NOT_RETRY.set(true);
    TableName tableName = TableName.valueOf(name.getMethodName());
    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      TEST_UTIL.loadTable(t, FAMILY, false);
      TEST_UTIL.getAdmin().flush(tableName);
      inject();
      TEST_UTIL.countRows(t, new Scan().addColumn(FAMILY, FAMILY));
      fail("Should have thrown an exception");
    } catch (DoNotRetryIOException expected) {
      // expected
    }
    assertTrue(REQ_COUNT.get() > 0);
  }

  /**
   * Tests the case where a coprocessor throws a regular IOException in the scan. The expectation is
   * that the we will keep on retrying, but fail after the retries are exhausted instead of retrying
   * indefinitely.
   */
  @Test
  public void testScannerFailsAfterRetriesWhenCoprocessorThrowsIOE()
      throws IOException, InterruptedException {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    TableName tableName = TableName.valueOf(name.getMethodName());
    reset();
    THROW_ONCE.set(false); // throw exceptions in every retry
    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      TEST_UTIL.loadTable(t, FAMILY, false);
      TEST_UTIL.getAdmin().flush(tableName);
      inject();
      TEST_UTIL.countRows(t, new Scan().addColumn(FAMILY, FAMILY));
      fail("Should have thrown an exception");
    } catch (DoNotRetryIOException expected) {
      assertThat(expected, instanceOf(ScannerResetException.class));
      // expected
    }
    assertTrue(REQ_COUNT.get() >= 3);
  }

}
