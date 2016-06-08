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

package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALKey;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.common.collect.Lists;

@Category(MediumTests.class)
public class TestRegionObserverForAddingMutationsFromCoprocessors {

  private static final Log LOG
    = LogFactory.getLog(TestRegionObserverForAddingMutationsFromCoprocessors.class);

  private static HBaseTestingUtility util;
  private static final byte[] dummy = Bytes.toBytes("dummy");
  private static final byte[] row1 = Bytes.toBytes("r1");
  private static final byte[] row2 = Bytes.toBytes("r2");
  private static final byte[] row3 = Bytes.toBytes("r3");
  private static final byte[] test = Bytes.toBytes("test");

  @Rule
  public TestName name = new TestName();
  private TableName tableName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, TestWALObserver.class.getName());
    util = new HBaseTestingUtility(conf);
    util.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    tableName = TableName.valueOf(name.getMethodName());
  }

  private void createTable(String coprocessor) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName)
        .addFamily(new HColumnDescriptor(dummy))
        .addFamily(new HColumnDescriptor(test))
        .addCoprocessor(coprocessor);
    util.getAdmin().createTable(htd);
  }

  /**
   * Test various multiput operations.
   * @throws Exception
   */
  @Test
  public void testMulti() throws Exception {
    createTable(TestMultiMutationCoprocessor.class.getName());

    try (Table t = util.getConnection().getTable(tableName)) {
      t.put(new Put(row1).addColumn(test, dummy, dummy));
      assertRowCount(t, 3);
    }
  }

  /**
   * Tests that added mutations from coprocessors end up in the WAL.
   */
  @Test
  public void testCPMutationsAreWrittenToWALEdit() throws Exception {
    createTable(TestMultiMutationCoprocessor.class.getName());

    try (Table t = util.getConnection().getTable(tableName)) {
      t.put(new Put(row1).addColumn(test, dummy, dummy));
      assertRowCount(t, 3);
    }

    assertNotNull(TestWALObserver.savedEdit);
    assertEquals(4, TestWALObserver.savedEdit.getCells().size());
  }

  private static void assertRowCount(Table t, int expected) throws IOException {
    try (ResultScanner scanner = t.getScanner(new Scan())) {
      int i = 0;
      for (Result r: scanner) {
        LOG.info(r.toString());
        i++;
      }
      assertEquals(expected, i);
    }
  }

  @Test
  public void testDeleteCell() throws Exception {
    createTable(TestDeleteCellCoprocessor.class.getName());

    try (Table t = util.getConnection().getTable(tableName)) {
      t.put(Lists.newArrayList(
        new Put(row1).addColumn(test, dummy, dummy),
        new Put(row2).addColumn(test, dummy, dummy),
        new Put(row3).addColumn(test, dummy, dummy)
          ));

      assertRowCount(t, 3);

      t.delete(new Delete(test).addColumn(test, dummy)); // delete non-existing row
      assertRowCount(t, 1);
    }
  }

  @Test
  public void testDeleteFamily() throws Exception {
    createTable(TestDeleteFamilyCoprocessor.class.getName());

    try (Table t = util.getConnection().getTable(tableName)) {
      t.put(Lists.newArrayList(
        new Put(row1).addColumn(test, dummy, dummy),
        new Put(row2).addColumn(test, dummy, dummy),
        new Put(row3).addColumn(test, dummy, dummy)
          ));

      assertRowCount(t, 3);

      t.delete(new Delete(test).addFamily(test)); // delete non-existing row
      assertRowCount(t, 1);
    }
  }

  @Test
  public void testDeleteRow() throws Exception {
    createTable(TestDeleteRowCoprocessor.class.getName());

    try (Table t = util.getConnection().getTable(tableName)) {
      t.put(Lists.newArrayList(
        new Put(row1).addColumn(test, dummy, dummy),
        new Put(row2).addColumn(test, dummy, dummy),
        new Put(row3).addColumn(test, dummy, dummy)
          ));

      assertRowCount(t, 3);

      t.delete(new Delete(test).addColumn(test, dummy)); // delete non-existing row
      assertRowCount(t, 1);
    }
  }

  public static class TestMultiMutationCoprocessor extends BaseRegionObserver {
    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
        MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      Mutation mut = miniBatchOp.getOperation(0);
      List<Cell> cells = mut.getFamilyCellMap().get(test);
      Put[] puts = new Put[] {
          new Put(row1).addColumn(test, dummy, cells.get(0).getTimestamp(),
            Bytes.toBytes("cpdummy")),
          new Put(row2).addColumn(test, dummy, cells.get(0).getTimestamp(), dummy),
          new Put(row3).addColumn(test, dummy, cells.get(0).getTimestamp(), dummy),
      };
      LOG.info("Putting:" + puts);
      miniBatchOp.addOperationsFromCP(0, puts);
    }
  }

  public static class TestDeleteCellCoprocessor extends BaseRegionObserver {
    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
        MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      Mutation mut = miniBatchOp.getOperation(0);

      if (mut instanceof Delete) {
        List<Cell> cells = mut.getFamilyCellMap().get(test);
        Delete[] deletes = new Delete[] {
            // delete only 2 rows
            new Delete(row1).addColumns(test, dummy, cells.get(0).getTimestamp()),
            new Delete(row2).addColumns(test, dummy, cells.get(0).getTimestamp()),
        };
        LOG.info("Deleting:" + Arrays.toString(deletes));
        miniBatchOp.addOperationsFromCP(0, deletes);
      }
    }
  }

  public static class TestDeleteFamilyCoprocessor extends BaseRegionObserver {
    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
        MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      Mutation mut = miniBatchOp.getOperation(0);

      if (mut instanceof Delete) {
        List<Cell> cells = mut.getFamilyCellMap().get(test);
        Delete[] deletes = new Delete[] {
            // delete only 2 rows
            new Delete(row1).addFamily(test, cells.get(0).getTimestamp()),
            new Delete(row2).addFamily(test, cells.get(0).getTimestamp()),
        };
        LOG.info("Deleting:" + Arrays.toString(deletes));
        miniBatchOp.addOperationsFromCP(0, deletes);
      }
    }
  }

  public static class TestDeleteRowCoprocessor extends BaseRegionObserver {
    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
        MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      Mutation mut = miniBatchOp.getOperation(0);

      if (mut instanceof Delete) {
        List<Cell> cells = mut.getFamilyCellMap().get(test);
        Delete[] deletes = new Delete[] {
            // delete only 2 rows
            new Delete(row1, cells.get(0).getTimestamp()),
            new Delete(row2, cells.get(0).getTimestamp()),
        };
        LOG.info("Deleting:" + Arrays.toString(deletes));
        miniBatchOp.addOperationsFromCP(0, deletes);
      }
    }
  }

  public static class TestWALObserver extends BaseWALObserver {
    static WALEdit savedEdit = null;
    @Override
    public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
        HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
      if (info.getTable().equals(TableName.valueOf("testCPMutationsAreWrittenToWALEdit"))) {
        savedEdit = logEdit;
      }
      super.postWALWrite(ctx, info, logKey, logEdit);
    }
  }
}
