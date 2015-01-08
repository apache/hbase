/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import static junit.framework.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, MediumTests.class})
/*
 * This test verifies that the scenarios illustrated by HBASE-10850 work
 * w.r.t. essential column family optimization
 */
public class TestSCVFWithMiniCluster {
  private static final TableName HBASE_TABLE_NAME = TableName.valueOf("TestSCVFWithMiniCluster");

  private static final byte[] FAMILY_A = Bytes.toBytes("a");
  private static final byte[] FAMILY_B = Bytes.toBytes("b");

  private static final byte[] QUALIFIER_FOO = Bytes.toBytes("foo");
  private static final byte[] QUALIFIER_BAR = Bytes.toBytes("bar");

  private static Table htable;

  private static Filter scanFilter;

  private int expected = 1;

  @BeforeClass
  public static void setUp() throws Exception {
    HBaseTestingUtility util = new HBaseTestingUtility();

    util.startMiniCluster(1);

    Admin admin = util.getHBaseAdmin();
    destroy(admin, HBASE_TABLE_NAME);
    create(admin, HBASE_TABLE_NAME, FAMILY_A, FAMILY_B);
    admin.close();
    htable = util.getConnection().getTable(HBASE_TABLE_NAME);

    /* Add some values */
    List<Put> puts = new ArrayList<Put>();

    /* Add a row with 'a:foo' = false */
    Put put = new Put(Bytes.toBytes("1"));
    put.setDurability(Durability.SKIP_WAL);
    put.add(FAMILY_A, QUALIFIER_FOO, Bytes.toBytes("false"));
    put.add(FAMILY_A, QUALIFIER_BAR, Bytes.toBytes("_flag_"));
    put.add(FAMILY_B, QUALIFIER_FOO, Bytes.toBytes("_flag_"));
    put.add(FAMILY_B, QUALIFIER_BAR, Bytes.toBytes("_flag_"));
    puts.add(put);

    /* Add a row with 'a:foo' = true */
    put = new Put(Bytes.toBytes("2"));
    put.setDurability(Durability.SKIP_WAL);
    put.add(FAMILY_A, QUALIFIER_FOO, Bytes.toBytes("true"));
    put.add(FAMILY_A, QUALIFIER_BAR, Bytes.toBytes("_flag_"));
    put.add(FAMILY_B, QUALIFIER_FOO, Bytes.toBytes("_flag_"));
    put.add(FAMILY_B, QUALIFIER_BAR, Bytes.toBytes("_flag_"));
    puts.add(put);

    /* Add a row with 'a:foo' qualifier not set */
    put = new Put(Bytes.toBytes("3"));
    put.setDurability(Durability.SKIP_WAL);
    put.add(FAMILY_A, QUALIFIER_BAR, Bytes.toBytes("_flag_"));
    put.add(FAMILY_B, QUALIFIER_FOO, Bytes.toBytes("_flag_"));
    put.add(FAMILY_B, QUALIFIER_BAR, Bytes.toBytes("_flag_"));
    puts.add(put);

    htable.put(puts);
    /*
     * We want to filter out from the scan all rows that do not have the column 'a:foo' with value
     * 'false'. Only row with key '1' should be returned in the scan.
     */
    scanFilter = new SingleColumnValueFilter(FAMILY_A, QUALIFIER_FOO, CompareOp.EQUAL,
      new BinaryComparator(Bytes.toBytes("false")));
    ((SingleColumnValueFilter) scanFilter).setFilterIfMissing(true);
  }
  
  @AfterClass
  public static void tearDown() throws Exception {
    htable.close();
  }

  private void verify(Scan scan) throws IOException {
    ResultScanner scanner = htable.getScanner(scan);
    Iterator<Result> it = scanner.iterator();

    /* Then */
    int count = 0;
    try {
      while (it.hasNext()) {
        it.next();
        count++;
      }
    } finally {
      scanner.close();
    }
    assertEquals(expected, count);
  }
  /**
   * Test the filter by adding all columns of family A in the scan. (OK)
   */
  @Test
  public void scanWithAllQualifiersOfFamiliyA() throws IOException {
    /* Given */
    Scan scan = new Scan();
    scan.addFamily(FAMILY_A);
    scan.setFilter(scanFilter);

    verify(scan);
  }

  /**
   * Test the filter by adding all columns of family A and B in the scan. (KO: row '3' without
   * 'a:foo' qualifier is returned)
   */
  @Test
  public void scanWithAllQualifiersOfBothFamilies() throws IOException {
    /* When */
    Scan scan = new Scan();
    scan.setFilter(scanFilter);

    verify(scan);
  }

  /**
   * Test the filter by adding 2 columns of family A and 1 column of family B in the scan. (KO: row
   * '3' without 'a:foo' qualifier is returned)
   */
  @Test
  public void scanWithSpecificQualifiers1() throws IOException {
    /* When */
    Scan scan = new Scan();
    scan.addColumn(FAMILY_A, QUALIFIER_FOO);
    scan.addColumn(FAMILY_A, QUALIFIER_BAR);
    scan.addColumn(FAMILY_B, QUALIFIER_BAR);
    scan.addColumn(FAMILY_B, QUALIFIER_FOO);
    scan.setFilter(scanFilter);

    verify(scan);
  }

  /**
   * Test the filter by adding 1 column of family A (the one used in the filter) and 1 column of
   * family B in the scan. (OK)
   */
  @Test
  public void scanWithSpecificQualifiers2() throws IOException {
    /* When */
    Scan scan = new Scan();
    scan.addColumn(FAMILY_A, QUALIFIER_FOO);
    scan.addColumn(FAMILY_B, QUALIFIER_BAR);
    scan.setFilter(scanFilter);

    verify(scan);
  }

  /**
   * Test the filter by adding 2 columns of family A in the scan. (OK)
   */
  @Test
  public void scanWithSpecificQualifiers3() throws IOException {
    /* When */
    Scan scan = new Scan();
    scan.addColumn(FAMILY_A, QUALIFIER_FOO);
    scan.addColumn(FAMILY_A, QUALIFIER_BAR);
    scan.setFilter(scanFilter);

    verify(scan);
  }

  private static void create(Admin admin, TableName tableName, byte[]... families)
      throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      HColumnDescriptor colDesc = new HColumnDescriptor(family);
      colDesc.setMaxVersions(1);
      colDesc.setCompressionType(Algorithm.GZ);
      desc.addFamily(colDesc);
    }
    try {
      admin.createTable(desc);
    } catch (TableExistsException tee) {
      /* Ignore */
    }
  }

  private static void destroy(Admin admin, TableName tableName) throws IOException {
    try {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    } catch (TableNotFoundException tnfe) {
      /* Ignore */
    }
  }
}
