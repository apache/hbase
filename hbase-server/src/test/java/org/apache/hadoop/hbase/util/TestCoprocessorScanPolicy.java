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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;

// this is deliberately not in the o.a.h.h.regionserver package

// in order to make sure all required classes/method are available

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.DelegatingInternalScanner;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category({ MiscTests.class, MediumTests.class })
@RunWith(Parameterized.class)
public class TestCoprocessorScanPolicy {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCoprocessorScanPolicy.class);

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] F = Bytes.toBytes("fam");
  private static final byte[] Q = Bytes.toBytes("qual");
  private static final byte[] R = Bytes.toBytes("row");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, ScanObserver.class.getName());
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Parameters
  public static Collection<Object[]> parameters() {
    return HBaseCommonTestingUtility.BOOLEAN_PARAMETERIZED;
  }

  public TestCoprocessorScanPolicy(boolean parallelSeekEnable) {
    TEST_UTIL.getMiniHBaseCluster().getConf()
        .setBoolean(StoreScanner.STORESCANNER_PARALLEL_SEEK_ENABLE, parallelSeekEnable);
  }

  @Test
  public void testBaseCases() throws Exception {
    TableName tableName = TableName.valueOf("baseCases");
    if (TEST_UTIL.getAdmin().tableExists(tableName)) {
      TEST_UTIL.deleteTable(tableName);
    }
    Table t = TEST_UTIL.createTable(tableName, F, 10);
    // insert 3 versions
    long now = EnvironmentEdgeManager.currentTime();
    Put p = new Put(R);
    p.addColumn(F, Q, now, Q);
    t.put(p);
    p = new Put(R);
    p.addColumn(F, Q, now + 1, Q);
    t.put(p);
    p = new Put(R);
    p.addColumn(F, Q, now + 2, Q);
    t.put(p);

    Get g = new Get(R);
    g.readVersions(10);
    Result r = t.get(g);
    assertEquals(3, r.size());

    TEST_UTIL.flush(tableName);
    TEST_UTIL.compact(tableName, true);

    // still visible after a flush/compaction
    r = t.get(g);
    assertEquals(3, r.size());

    // set the version override to 2
    p = new Put(R);
    p.setAttribute("versions", new byte[] {});
    p.addColumn(F, tableName.getName(), Bytes.toBytes(2));
    t.put(p);

    // only 2 versions now
    r = t.get(g);
    assertEquals(2, r.size());

    TEST_UTIL.flush(tableName);
    TEST_UTIL.compact(tableName, true);

    // still 2 versions after a flush/compaction
    r = t.get(g);
    assertEquals(2, r.size());

    // insert a new version
    p.addColumn(F, Q, now + 3, Q);
    t.put(p);

    // still 2 versions
    r = t.get(g);
    assertEquals(2, r.size());

    t.close();
  }

  @Test
  public void testTTL() throws Exception {
    TableName tableName = TableName.valueOf("testTTL");
    if (TEST_UTIL.getAdmin().tableExists(tableName)) {
      TEST_UTIL.deleteTable(tableName);
    }
    Table t = TEST_UTIL.createTable(tableName, F, 10);
    long now = EnvironmentEdgeManager.currentTime();
    ManualEnvironmentEdge me = new ManualEnvironmentEdge();
    me.setValue(now);
    EnvironmentEdgeManagerTestHelper.injectEdge(me);
    // 2s in the past
    long ts = now - 2000;

    Put p = new Put(R);
    p.addColumn(F, Q, ts, Q);
    t.put(p);
    p = new Put(R);
    p.addColumn(F, Q, ts + 1, Q);
    t.put(p);

    // Set the TTL override to 3s
    p = new Put(R);
    p.setAttribute("ttl", new byte[] {});
    p.addColumn(F, tableName.getName(), Bytes.toBytes(3000L));
    t.put(p);
    // these two should still be there
    Get g = new Get(R);
    g.readAllVersions();
    Result r = t.get(g);
    // still there?
    assertEquals(2, r.size());

    TEST_UTIL.flush(tableName);
    TEST_UTIL.compact(tableName, true);

    g = new Get(R);
    g.readAllVersions();
    r = t.get(g);
    // still there?
    assertEquals(2, r.size());

    // roll time forward 2s.
    me.setValue(now + 2000);
    // now verify that data eventually does expire
    g = new Get(R);
    g.readAllVersions();
    r = t.get(g);
    // should be gone now
    assertEquals(0, r.size());
    t.close();
    EnvironmentEdgeManager.reset();
  }

  public static class ScanObserver implements RegionCoprocessor, RegionObserver {
    private final ConcurrentMap<TableName, Long> ttls = new ConcurrentHashMap<>();
    private final ConcurrentMap<TableName, Integer> versions = new ConcurrentHashMap<>();

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    // lame way to communicate with the coprocessor,
    // since it is loaded by a different class loader
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c, final Put put,
        final WALEdit edit, final Durability durability) throws IOException {
      if (put.getAttribute("ttl") != null) {
        Cell cell = put.getFamilyCellMap().values().stream().findFirst().get().get(0);
        ttls.put(
          TableName.valueOf(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength())),
          Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        c.bypass();
      } else if (put.getAttribute("versions") != null) {
        Cell cell = put.getFamilyCellMap().values().stream().findFirst().get().get(0);
        versions.put(
          TableName.valueOf(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength())),
          Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        c.bypass();
      }
    }

    private InternalScanner wrap(Store store, InternalScanner scanner) {
      Long ttl = this.ttls.get(store.getTableName());
      Integer version = this.versions.get(store.getTableName());
      return new DelegatingInternalScanner(scanner) {

        private byte[] row;

        private byte[] qualifier;

        private int count;

        private Predicate<Cell> checkTtl(long now, long ttl) {
          return c -> now - c.getTimestamp() > ttl;
        }

        private Predicate<Cell> checkVersion(Cell firstCell, int version) {
          if (version == 0) {
            return c -> true;
          } else {
            if (row == null || !CellUtil.matchingRows(firstCell, row)) {
              row = CellUtil.cloneRow(firstCell);
              // reset qualifier as there is a row change
              qualifier = null;
            }
            return c -> {
              if (qualifier != null && CellUtil.matchingQualifier(c, qualifier)) {
                if (count >= version) {
                  return true;
                }
                count++;
                return false;
              } else { // qualifier switch
                qualifier = CellUtil.cloneQualifier(c);
                count = 1;
                return false;
              }
            };
          }
        }

        @Override
        public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
          boolean moreRows = scanner.next(result, scannerContext);
          if (result.isEmpty()) {
            return moreRows;
          }
          long now = EnvironmentEdgeManager.currentTime();
          Predicate<Cell> predicate = null;
          if (ttl != null) {
            predicate = checkTtl(now, ttl);
          }
          if (version != null) {
            Predicate<Cell> vp = checkVersion(result.get(0), version);
            if (predicate != null) {
              predicate = predicate.and(vp);
            } else {
              predicate = vp;
            }
          }
          if (predicate != null) {
            result.removeIf(predicate);
          }
          return moreRows;
        }
      };
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
      return wrap(store, scanner);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
        CompactionRequest request) throws IOException {
      return wrap(store, scanner);
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
        List<Cell> result) throws IOException {
      TableName tableName = c.getEnvironment().getRegion().getTableDescriptor().getTableName();
      Long ttl = this.ttls.get(tableName);
      if (ttl != null) {
        get.setTimeRange(EnvironmentEdgeManager.currentTime() - ttl, get.getTimeRange().getMax());
      }
      Integer version = this.versions.get(tableName);
      if (version != null) {
        get.readVersions(version);
      }
    }

    @Override
    public void preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan)
        throws IOException {
      Region region = c.getEnvironment().getRegion();
      TableName tableName = region.getTableDescriptor().getTableName();
      Long ttl = this.ttls.get(tableName);
      if (ttl != null) {
        scan.setTimeRange(EnvironmentEdgeManager.currentTime() - ttl, scan.getTimeRange().getMax());
      }
      Integer version = this.versions.get(tableName);
      if (version != null) {
        scan.readVersions(version);
      }
    }
  }
}
