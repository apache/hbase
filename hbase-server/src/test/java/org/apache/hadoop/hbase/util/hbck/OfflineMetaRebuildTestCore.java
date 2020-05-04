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
package org.apache.hadoop.hbase.util.hbck;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This testing base class creates a minicluster and testing table table
 * and shuts down the cluster afterwards. It also provides methods wipes out
 * meta and to inject errors into meta and the file system.
 *
 * Tests should generally break stuff, then attempt to rebuild the meta table
 * offline, then restart hbase, and finally perform checks.
 *
 * NOTE: This is a slow set of tests which takes ~30s each needs to run on a
 * relatively beefy machine. It seems necessary to have each test in a new jvm
 * since minicluster startup and tear downs seem to leak file handles and
 * eventually cause out of file handle exceptions.
 */
@Category({MiscTests.class, LargeTests.class})
public class OfflineMetaRebuildTestCore {
  private final static Logger LOG = LoggerFactory
      .getLogger(OfflineMetaRebuildTestCore.class);
  protected HBaseTestingUtility TEST_UTIL;
  protected Configuration conf;
  private final static byte[] FAM = Bytes.toBytes("fam");

  // for the instance, reset every test run
  protected Table htbl;
  protected final static byte[][] splits = new byte[][] { Bytes.toBytes("A"),
      Bytes.toBytes("B"), Bytes.toBytes("C") };

  private final static String TABLE_BASE = "tableMetaRebuild";
  private static int tableIdx = 0;
  protected TableName table = TableName.valueOf("tableMetaRebuild");
  protected Connection connection;

  @Before
  public void setUpBefore() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().setInt("dfs.datanode.max.xceivers", 9192);
    TEST_UTIL.startMiniCluster(3);
    conf = TEST_UTIL.getConfiguration();
    this.connection = ConnectionFactory.createConnection(conf);
    assertEquals(0, TEST_UTIL.getAdmin().listTableDescriptors().size());

    // setup the table
    table = TableName.valueOf(TABLE_BASE + "-" + tableIdx);
    tableIdx++;
    htbl = setupTable(table);
    populateTable(htbl);
    assertEquals(5, scanMeta());
    LOG.info("Table " + table + " has " + tableRowCount(conf, table)
        + " entries.");
    assertEquals(16, tableRowCount(conf, table));
    TEST_UTIL.getAdmin().disableTable(table);
    assertEquals(1, TEST_UTIL.getAdmin().listTableDescriptors().size());
  }

  @After
  public void tearDownAfter() throws Exception {
    if (this.htbl != null) {
      this.htbl.close();
      this.htbl = null;
    }
    this.connection.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Setup a clean table before we start mucking with it.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  private Table setupTable(TableName tablename) throws Exception {
    TableDescriptorBuilder tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(tablename);
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(FAM).build();
    // If a table has no CF's it doesn't get checked
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    TEST_UTIL.getAdmin().createTable(tableDescriptorBuilder.build(), splits);
    return this.connection.getTable(tablename);
  }

  private void dumpMeta(TableDescriptor htd) throws IOException {
    List<byte[]> metaRows = TEST_UTIL.getMetaTableRows(htd.getTableName());
    for (byte[] row : metaRows) {
      LOG.info(Bytes.toString(row));
    }
  }

  private void populateTable(Table tbl) throws IOException {
    byte[] values = { 'A', 'B', 'C', 'D' };
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      for (int j = 0; j < values.length; j++) {
        Put put = new Put(new byte[] { values[i], values[j] });
        put.addColumn(Bytes.toBytes("fam"), new byte[]{}, new byte[]{values[i],
                values[j]});
        puts.add(put);
      }
    }
    tbl.put(puts);
  }

  protected void deleteRegion(Configuration conf, final Table tbl,
      byte[] startKey, byte[] endKey) throws IOException {

    LOG.info("Before delete:");
    TableDescriptor htd = tbl.getDescriptor();
    dumpMeta(htd);

    List<HRegionLocation> regions;
    try(RegionLocator rl = connection.getRegionLocator(tbl.getName())) {
      regions = rl.getAllRegionLocations();
    }

    for (HRegionLocation e : regions) {
      RegionInfo hri = e.getRegion();
      ServerName hsa = e.getServerName();
      if (Bytes.compareTo(hri.getStartKey(), startKey) == 0
          && Bytes.compareTo(hri.getEndKey(), endKey) == 0) {

        LOG.info("RegionName: " + hri.getRegionNameAsString());
        byte[] deleteRow = hri.getRegionName();
        TEST_UTIL.getAdmin().unassign(deleteRow, true);

        LOG.info("deleting hdfs data: " + hri.toString() + hsa.toString());
        Path rootDir = CommonFSUtils.getRootDir(conf);
        FileSystem fs = rootDir.getFileSystem(conf);
        Path p = new Path(CommonFSUtils.getTableDir(rootDir, htd.getTableName()),
            hri.getEncodedName());
        fs.delete(p, true);

        try (Table meta = this.connection.getTable(TableName.META_TABLE_NAME)) {
          Delete delete = new Delete(deleteRow);
          meta.delete(delete);
        }
      }
      LOG.info(hri.toString() + hsa.toString());
    }

    TEST_UTIL.getMetaTableRows(htd.getTableName());
    LOG.info("After delete:");
    dumpMeta(htd);
  }

  protected RegionInfo createRegion(Configuration conf, final Table htbl,
      byte[] startKey, byte[] endKey) throws IOException {
    Table meta = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
    RegionInfo hri = RegionInfoBuilder.newBuilder(htbl.getName())
        .setStartKey(startKey)
        .setEndKey(endKey)
        .build();

    LOG.info("manually adding regioninfo and hdfs data: " + hri.toString());
    Path rootDir = CommonFSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);
    Path p = new Path(CommonFSUtils.getTableDir(rootDir, htbl.getName()),
        hri.getEncodedName());
    fs.mkdirs(p);
    Path riPath = new Path(p, HRegionFileSystem.REGION_INFO_FILE);
    FSDataOutputStream out = fs.create(riPath);
    out.write(RegionInfo.toDelimitedByteArray(hri));
    out.close();

    // add to meta.
    MetaTableAccessor.addRegionToMeta(TEST_UTIL.getConnection(), hri);
    meta.close();
    return hri;
  }

  protected void wipeOutMeta() throws IOException {
    // Mess it up by blowing up meta.
    Admin admin = TEST_UTIL.getAdmin();
    Scan s = new Scan();
    Table meta = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
    ResultScanner scanner = meta.getScanner(s);
    List<Delete> dels = new ArrayList<>();
    for (Result r : scanner) {
      RegionInfo info =
          MetaTableAccessor.getRegionInfo(r);
      if(info != null && !info.getTable().getNamespaceAsString()
          .equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR)) {
        Delete d = new Delete(r.getRow());
        dels.add(d);
        admin.unassign(r.getRow(), true);
      }
    }
    meta.delete(dels);
    scanner.close();
    meta.close();
  }

  /**
   * Returns the number of rows in a given table. HBase must be up and the table
   * should be present (will wait for timeout for a while otherwise)
   *
   * @return # of rows in the specified table
   */
  protected int tableRowCount(Configuration conf, TableName table)
      throws IOException {
    Table t = TEST_UTIL.getConnection().getTable(table);
    Scan st = new Scan();

    ResultScanner rst = t.getScanner(st);
    int count = 0;
    for (@SuppressWarnings("unused")
    Result rt : rst) {
      count++;
    }
    t.close();
    return count;
  }

  /**
   * Dumps hbase:meta table info
   *
   * @return # of entries in meta.
   */
  protected int scanMeta() throws IOException {
    LOG.info("Scanning META");
    MetaTableAccessor.fullScanMetaAndPrint(TEST_UTIL.getConnection());
    return MetaTableAccessor.fullScanRegions(TEST_UTIL.getConnection()).size();
  }

  protected List<TableDescriptor> getTables(final Configuration configuration) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(configuration)) {
      try (Admin admin = connection.getAdmin()) {
        return admin.listTableDescriptors();
      }
    }
  }
}
