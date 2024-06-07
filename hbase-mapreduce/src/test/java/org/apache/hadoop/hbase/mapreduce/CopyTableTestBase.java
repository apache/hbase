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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.mob.MobTestUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Base class for testing CopyTable MR tool.
 */
public abstract class CopyTableTestBase {

  protected static final byte[] ROW1 = Bytes.toBytes("row1");
  protected static final byte[] ROW2 = Bytes.toBytes("row2");
  protected static final String FAMILY_A_STRING = "a";
  protected static final String FAMILY_B_STRING = "b";
  protected static final byte[] FAMILY_A = Bytes.toBytes(FAMILY_A_STRING);
  protected static final byte[] FAMILY_B = Bytes.toBytes(FAMILY_B_STRING);
  protected static final byte[] QUALIFIER = Bytes.toBytes("q");

  @Rule
  public TestName name = new TestName();

  protected abstract Table createSourceTable(TableDescriptor desc) throws Exception;

  protected abstract Table createTargetTable(TableDescriptor desc) throws Exception;

  protected abstract void dropSourceTable(TableName tableName) throws Exception;

  protected abstract void dropTargetTable(TableName tableName) throws Exception;

  protected abstract String[] getPeerClusterOptions() throws Exception;

  protected final void loadData(Table t, byte[] family, byte[] column) throws IOException {
    for (int i = 0; i < 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put p = new Put(row);
      p.addColumn(family, column, row);
      t.put(p);
    }
  }

  protected final void verifyRows(Table t, byte[] family, byte[] column) throws IOException {
    for (int i = 0; i < 10; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Get g = new Get(row).addFamily(family);
      Result r = t.get(g);
      assertNotNull(r);
      assertEquals(1, r.size());
      Cell cell = r.rawCells()[0];
      assertTrue(CellUtil.matchingQualifier(cell, column));
      assertEquals(Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength(), row, 0, row.length), 0);
    }
  }

  protected final void doCopyTableTest(Configuration conf, boolean bulkload) throws Exception {
    TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
    TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");
    byte[] family = Bytes.toBytes("family");
    byte[] column = Bytes.toBytes("c1");
    TableDescriptor desc1 = TableDescriptorBuilder.newBuilder(tableName1)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
    TableDescriptor desc2 = TableDescriptorBuilder.newBuilder(tableName2)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();

    try (Table t1 = createSourceTable(desc1); Table t2 = createTargetTable(desc2)) {
      // put rows into the first table
      loadData(t1, family, column);

      String[] peerClusterOptions = getPeerClusterOptions();
      if (bulkload) {
        assertTrue(runCopy(conf,
          ArrayUtils.addAll(peerClusterOptions, "--new.name=" + tableName2.getNameAsString(),
            "--bulkload", tableName1.getNameAsString())));
      } else {
        assertTrue(runCopy(conf, ArrayUtils.addAll(peerClusterOptions,
          "--new.name=" + tableName2.getNameAsString(), tableName1.getNameAsString())));
      }

      // verify the data was copied into table 2
      verifyRows(t2, family, column);
    } finally {
      dropSourceTable(tableName1);
      dropTargetTable(tableName2);
    }
  }

  protected final void doCopyTableTestWithMob(Configuration conf, boolean bulkload)
    throws Exception {
    TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
    TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");
    byte[] family = Bytes.toBytes("mob");
    byte[] column = Bytes.toBytes("c1");

    ColumnFamilyDescriptorBuilder cfd = ColumnFamilyDescriptorBuilder.newBuilder(family);

    cfd.setMobEnabled(true);
    cfd.setMobThreshold(5);
    TableDescriptor desc1 =
      TableDescriptorBuilder.newBuilder(tableName1).setColumnFamily(cfd.build()).build();
    TableDescriptor desc2 =
      TableDescriptorBuilder.newBuilder(tableName2).setColumnFamily(cfd.build()).build();

    try (Table t1 = createSourceTable(desc1); Table t2 = createTargetTable(desc2)) {
      // put rows into the first table
      for (int i = 0; i < 10; i++) {
        Put p = new Put(Bytes.toBytes("row" + i));
        p.addColumn(family, column, column);
        t1.put(p);
      }

      String[] peerClusterOptions = getPeerClusterOptions();
      if (bulkload) {
        assertTrue(runCopy(conf,
          ArrayUtils.addAll(peerClusterOptions, "--new.name=" + tableName2.getNameAsString(),
            "--bulkload", tableName1.getNameAsString())));
      } else {
        assertTrue(runCopy(conf, ArrayUtils.addAll(peerClusterOptions,
          "--new.name=" + tableName2.getNameAsString(), tableName1.getNameAsString())));
      }

      // verify the data was copied into table 2
      for (int i = 0; i < 10; i++) {
        Get g = new Get(Bytes.toBytes("row" + i));
        Result r = t2.get(g);
        assertEquals(1, r.size());
        assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], column));
        assertEquals("compare row values between two tables",
          t1.getDescriptor().getValue("row" + i), t2.getDescriptor().getValue("row" + i));
      }

      assertEquals("compare count of mob rows after table copy", MobTestUtil.countMobRows(t1),
        MobTestUtil.countMobRows(t2));
      assertEquals("compare count of mob row values between two tables",
        t1.getDescriptor().getValues().size(), t2.getDescriptor().getValues().size());
      assertTrue("The mob row count is 0 but should be > 0", MobTestUtil.countMobRows(t2) > 0);
    } finally {
      dropSourceTable(tableName1);
      dropTargetTable(tableName2);
    }
  }

  protected final boolean runCopy(Configuration conf, String[] args) throws Exception {
    int status = ToolRunner.run(conf, new CopyTable(), args);
    return status == 0;
  }

  protected final void testStartStopRow(Configuration conf) throws Exception {
    final TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
    final TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");
    final byte[] family = Bytes.toBytes("family");
    final byte[] column = Bytes.toBytes("c1");
    final byte[] row0 = Bytes.toBytesBinary("\\x01row0");
    final byte[] row1 = Bytes.toBytesBinary("\\x01row1");
    final byte[] row2 = Bytes.toBytesBinary("\\x01row2");
    TableDescriptor desc1 = TableDescriptorBuilder.newBuilder(tableName1)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
    TableDescriptor desc2 = TableDescriptorBuilder.newBuilder(tableName2)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
    try (Table t1 = createSourceTable(desc1); Table t2 = createTargetTable(desc2)) {
      // put rows into the first table
      Put p = new Put(row0);
      p.addColumn(family, column, column);
      t1.put(p);
      p = new Put(row1);
      p.addColumn(family, column, column);
      t1.put(p);
      p = new Put(row2);
      p.addColumn(family, column, column);
      t1.put(p);

      String[] peerClusterOptions = getPeerClusterOptions();
      assertTrue(runCopy(conf, ArrayUtils.addAll(peerClusterOptions, "--new.name=" + tableName2,
        "--startrow=\\x01row1", "--stoprow=\\x01row2", tableName1.getNameAsString())));

      // verify the data was copied into table 2
      // row1 exist, row0, row2 do not exist
      Get g = new Get(row1);
      Result r = t2.get(g);
      assertEquals(1, r.size());
      assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], column));

      g = new Get(row0);
      r = t2.get(g);
      assertEquals(0, r.size());

      g = new Get(row2);
      r = t2.get(g);
      assertEquals(0, r.size());
    } finally {
      dropSourceTable(tableName1);
      dropTargetTable(tableName2);
    }
  }

  protected final void testRenameFamily(Configuration conf) throws Exception {
    TableName sourceTable = TableName.valueOf(name.getMethodName() + "-source");
    TableName targetTable = TableName.valueOf(name.getMethodName() + "-target");

    TableDescriptor desc1 = TableDescriptorBuilder.newBuilder(sourceTable)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_A))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_B)).build();
    TableDescriptor desc2 = TableDescriptorBuilder.newBuilder(targetTable)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_A))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY_B)).build();

    try (Table t = createSourceTable(desc1); Table t2 = createTargetTable(desc2)) {
      Put p = new Put(ROW1);
      p.addColumn(FAMILY_A, QUALIFIER, Bytes.toBytes("Data11"));
      p.addColumn(FAMILY_B, QUALIFIER, Bytes.toBytes("Data12"));
      p.addColumn(FAMILY_A, QUALIFIER, Bytes.toBytes("Data13"));
      t.put(p);
      p = new Put(ROW2);
      p.addColumn(FAMILY_B, QUALIFIER, Bytes.toBytes("Dat21"));
      p.addColumn(FAMILY_A, QUALIFIER, Bytes.toBytes("Data22"));
      p.addColumn(FAMILY_B, QUALIFIER, Bytes.toBytes("Data23"));
      t.put(p);

      long currentTime = EnvironmentEdgeManager.currentTime();
      String[] args = ArrayUtils.addAll(getPeerClusterOptions(), "--new.name=" + targetTable,
        "--families=a:b", "--all.cells", "--starttime=" + (currentTime - 100000),
        "--endtime=" + (currentTime + 100000), "--versions=1", sourceTable.getNameAsString());
      assertNull(t2.get(new Get(ROW1)).getRow());

      assertTrue(runCopy(conf, args));

      assertNotNull(t2.get(new Get(ROW1)).getRow());
      Result res = t2.get(new Get(ROW1));
      byte[] b1 = res.getValue(FAMILY_B, QUALIFIER);
      assertEquals("Data13", Bytes.toString(b1));
      assertNotNull(t2.get(new Get(ROW2)).getRow());
      res = t2.get(new Get(ROW2));
      b1 = res.getValue(FAMILY_A, QUALIFIER);
      // Data from the family of B is not copied
      assertNull(b1);
    } finally {
      dropSourceTable(sourceTable);
      dropTargetTable(targetTable);
    }
  }
}
