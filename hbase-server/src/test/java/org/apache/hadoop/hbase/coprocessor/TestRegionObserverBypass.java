/*
 *
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

import static junit.framework.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({CoprocessorTests.class, MediumTests.class})
public class TestRegionObserverBypass {
  private static HBaseTestingUtility util;
  private static final TableName tableName = TableName.valueOf("test");
  private static final byte[] dummy = Bytes.toBytes("dummy");
  private static final byte[] row1 = Bytes.toBytes("r1");
  private static final byte[] row2 = Bytes.toBytes("r2");
  private static final byte[] row3 = Bytes.toBytes("r3");
  private static final byte[] test = Bytes.toBytes("test");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        TestCoprocessor.class.getName());
    util = new HBaseTestingUtility(conf);
    util.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    Admin admin = util.getAdmin();
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
    util.createTable(tableName, new byte[][] {dummy, test});
  }

  /**
   * do a single put that is bypassed by a RegionObserver
   * @throws Exception
   */
  @Test
  public void testSimple() throws Exception {
    Table t = util.getConnection().getTable(tableName);
    Put p = new Put(row1);
    p.addColumn(test, dummy, dummy);
    // before HBASE-4331, this would throw an exception
    t.put(p);
    checkRowAndDelete(t,row1,0);
    t.close();
  }

  /**
   * Test various multiput operations.
   * @throws Exception
   */
  @Test
  public void testMulti() throws Exception {
    //ensure that server time increments every time we do an operation, otherwise
    //previous deletes will eclipse successive puts having the same timestamp
    EnvironmentEdgeManagerTestHelper.injectEdge(new IncrementingEnvironmentEdge());

    Table t = util.getConnection().getTable(tableName);
    List<Put> puts = new ArrayList<>();
    Put p = new Put(row1);
    p.addColumn(dummy, dummy, dummy);
    puts.add(p);
    p = new Put(row2);
    p.addColumn(test, dummy, dummy);
    puts.add(p);
    p = new Put(row3);
    p.addColumn(test, dummy, dummy);
    puts.add(p);
    // before HBASE-4331, this would throw an exception
    t.put(puts);
    checkRowAndDelete(t,row1,1);
    checkRowAndDelete(t,row2,0);
    checkRowAndDelete(t,row3,0);

    puts.clear();
    p = new Put(row1);
    p.addColumn(test, dummy, dummy);
    puts.add(p);
    p = new Put(row2);
    p.addColumn(test, dummy, dummy);
    puts.add(p);
    p = new Put(row3);
    p.addColumn(test, dummy, dummy);
    puts.add(p);
    // before HBASE-4331, this would throw an exception
    t.put(puts);
    checkRowAndDelete(t,row1,0);
    checkRowAndDelete(t,row2,0);
    checkRowAndDelete(t,row3,0);

    puts.clear();
    p = new Put(row1);
    p.addColumn(test, dummy, dummy);
    puts.add(p);
    p = new Put(row2);
    p.addColumn(test, dummy, dummy);
    puts.add(p);
    p = new Put(row3);
    p.addColumn(dummy, dummy, dummy);
    puts.add(p);
    // this worked fine even before HBASE-4331
    t.put(puts);
    checkRowAndDelete(t,row1,0);
    checkRowAndDelete(t,row2,0);
    checkRowAndDelete(t,row3,1);

    puts.clear();
    p = new Put(row1);
    p.addColumn(dummy, dummy, dummy);
    puts.add(p);
    p = new Put(row2);
    p.addColumn(test, dummy, dummy);
    puts.add(p);
    p = new Put(row3);
    p.addColumn(dummy, dummy, dummy);
    puts.add(p);
    // this worked fine even before HBASE-4331
    t.put(puts);
    checkRowAndDelete(t,row1,1);
    checkRowAndDelete(t,row2,0);
    checkRowAndDelete(t,row3,1);

    puts.clear();
    p = new Put(row1);
    p.addColumn(test, dummy, dummy);
    puts.add(p);
    p = new Put(row2);
    p.addColumn(dummy, dummy, dummy);
    puts.add(p);
    p = new Put(row3);
    p.addColumn(test, dummy, dummy);
    puts.add(p);
    // before HBASE-4331, this would throw an exception
    t.put(puts);
    checkRowAndDelete(t,row1,0);
    checkRowAndDelete(t,row2,1);
    checkRowAndDelete(t,row3,0);
    t.close();

    EnvironmentEdgeManager.reset();
  }

  private void checkRowAndDelete(Table t, byte[] row, int count) throws IOException {
    Get g = new Get(row);
    Result r = t.get(g);
    assertEquals(count, r.size());
    Delete d = new Delete(row);
    t.delete(d);
  }

  public static class TestCoprocessor implements RegionObserver {
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Put put, final WALEdit edit, final Durability durability)
        throws IOException {
      Map<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
      if (familyMap.containsKey(test)) {
        e.bypass();
      }
    }
  }
}