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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

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
@Category(LargeTests.class)
public class OfflineMetaRebuildTestCore {
  protected final static Log LOG = LogFactory
      .getLog(OfflineMetaRebuildTestCore.class);
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
    assertEquals(0, TEST_UTIL.getHBaseAdmin().listTables().length);

    // setup the table
    table = TableName.valueOf(TABLE_BASE + "-" + tableIdx);
    tableIdx++;
    htbl = setupTable(table);
    populateTable(htbl);
    assertEquals(5, scanMeta());
    LOG.info("Table " + table + " has " + tableRowCount(conf, table)
        + " entries.");
    assertEquals(16, tableRowCount(conf, table));
    TEST_UTIL.getHBaseAdmin().disableTable(table);
    assertEquals(1, TEST_UTIL.getHBaseAdmin().listTables().length);
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
    HTableDescriptor desc = new HTableDescriptor(tablename);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toString(FAM));
    desc.addFamily(hcd); // If a table has no CF's it doesn't get checked
    TEST_UTIL.getHBaseAdmin().createTable(desc, splits);
    return this.connection.getTable(tablename);
  }

  private void dumpMeta(HTableDescriptor htd) throws IOException {
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
        put.add(Bytes.toBytes("fam"), new byte[] {}, new byte[] { values[i],
            values[j] });
        puts.add(put);
      }
    }
    tbl.put(puts);
  }

  /**
   * delete table in preparation for next test
   *
   * @param tablename
   * @throws IOException
   */
  void deleteTable(HBaseAdmin admin, String tablename) throws IOException {
    try {
      byte[] tbytes = Bytes.toBytes(tablename);
      admin.disableTable(tbytes);
      admin.deleteTable(tbytes);
    } catch (Exception e) {
      // Do nothing.
    }
  }

  protected void deleteRegion(Configuration conf, final Table tbl,
      byte[] startKey, byte[] endKey) throws IOException {

    LOG.info("Before delete:");
    HTableDescriptor htd = tbl.getTableDescriptor();
    dumpMeta(htd);

    Map<HRegionInfo, ServerName> hris = ((HTable)tbl).getRegionLocations();
    for (Entry<HRegionInfo, ServerName> e : hris.entrySet()) {
      HRegionInfo hri = e.getKey();
      ServerName hsa = e.getValue();
      if (Bytes.compareTo(hri.getStartKey(), startKey) == 0
          && Bytes.compareTo(hri.getEndKey(), endKey) == 0) {

        LOG.info("RegionName: " + hri.getRegionNameAsString());
        byte[] deleteRow = hri.getRegionName();
        TEST_UTIL.getHBaseAdmin().unassign(deleteRow, true);

        LOG.info("deleting hdfs data: " + hri.toString() + hsa.toString());
        Path rootDir = FSUtils.getRootDir(conf);
        FileSystem fs = rootDir.getFileSystem(conf);
        Path p = new Path(FSUtils.getTableDir(rootDir, htd.getTableName()),
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

  protected HRegionInfo createRegion(Configuration conf, final Table htbl,
      byte[] startKey, byte[] endKey) throws IOException {
    Table meta = new HTable(conf, TableName.META_TABLE_NAME);
    HTableDescriptor htd = htbl.getTableDescriptor();
    HRegionInfo hri = new HRegionInfo(htbl.getName(), startKey, endKey);

    LOG.info("manually adding regioninfo and hdfs data: " + hri.toString());
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);
    Path p = new Path(FSUtils.getTableDir(rootDir, htbl.getName()),
        hri.getEncodedName());
    fs.mkdirs(p);
    Path riPath = new Path(p, HRegionFileSystem.REGION_INFO_FILE);
    FSDataOutputStream out = fs.create(riPath);
    out.write(hri.toDelimitedByteArray());
    out.close();

    // add to meta.
    MetaTableAccessor.addRegionToMeta(meta, hri);
    meta.close();
    return hri;
  }

  protected void wipeOutMeta() throws IOException {
    // Mess it up by blowing up meta.
    Admin admin = TEST_UTIL.getHBaseAdmin();
    Scan s = new Scan();
    Table meta = new HTable(conf, TableName.META_TABLE_NAME);
    ResultScanner scanner = meta.getScanner(s);
    List<Delete> dels = new ArrayList<Delete>();
    for (Result r : scanner) {
      HRegionInfo info =
          HRegionInfo.getHRegionInfo(r);
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
    Table t = new HTable(conf, table);
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
    int count = 0;
    HTable meta = new HTable(conf, TableName.META_TABLE_NAME);
    ResultScanner scanner = meta.getScanner(new Scan());
    LOG.info("Table: " + Bytes.toString(meta.getTableName()));
    for (Result res : scanner) {
      LOG.info(Bytes.toString(res.getRow()));
      count++;
    }
    meta.close();
    return count;
  }

  protected HTableDescriptor[] getTables(final Configuration configuration) throws IOException {
    HTableDescriptor[] htbls = null;
    try (Connection connection = ConnectionFactory.createConnection(configuration)) {
      try (Admin admin = connection.getAdmin()) {
        htbls = admin.listTables();
      }
    }
    return htbls;
  }
}
