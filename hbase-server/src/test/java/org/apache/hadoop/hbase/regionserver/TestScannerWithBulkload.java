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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.regionserver.HStoreFile.BULKLOAD_TIME_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.MAX_SEQ_ID_KEY;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({RegionServerTests.class, MediumTests.class})
public class TestScannerWithBulkload {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestScannerWithBulkload.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  private static void createTable(Admin admin, TableName tableName) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    hcd.setMaxVersions(3);
    desc.addFamily(hcd);
    admin.createTable(desc);
  }

  @Test
  public void testBulkLoad() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    long l = System.currentTimeMillis();
    Admin admin = TEST_UTIL.getAdmin();
    createTable(admin, tableName);
    Scan scan = createScan();
    final Table table = init(admin, l, scan, tableName);
    // use bulkload
    final Path hfilePath = writeToHFile(l, "/temp/testBulkLoad/", "/temp/testBulkLoad/col/file",
      false);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.mapreduce.bulkload.assign.sequenceNumbers", true);
    BulkLoadHFiles.create(conf).bulkLoad(tableName, hfilePath);
    ResultScanner scanner = table.getScanner(scan);
    Result result = scanner.next();
    result = scanAfterBulkLoad(scanner, result, "version2");
    Put put0 = new Put(Bytes.toBytes("row1"));
    put0.add(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("col"), Bytes.toBytes("q"), l, Bytes
        .toBytes("version3")));
    table.put(put0);
    admin.flush(tableName);
    scanner = table.getScanner(scan);
    result = scanner.next();
    while (result != null) {
      List<Cell> cells = result.getColumnCells(Bytes.toBytes("col"), Bytes.toBytes("q"));
      for (Cell _c : cells) {
        if (Bytes.toString(_c.getRowArray(), _c.getRowOffset(), _c.getRowLength())
            .equals("row1")) {
          System.out
              .println(Bytes.toString(_c.getRowArray(), _c.getRowOffset(), _c.getRowLength()));
          System.out.println(Bytes.toString(_c.getQualifierArray(), _c.getQualifierOffset(),
            _c.getQualifierLength()));
          System.out.println(
            Bytes.toString(_c.getValueArray(), _c.getValueOffset(), _c.getValueLength()));
          Assert.assertEquals("version3",
            Bytes.toString(_c.getValueArray(), _c.getValueOffset(), _c.getValueLength()));
        }
      }
      result = scanner.next();
    }
    scanner.close();
    table.close();
  }

  private Result scanAfterBulkLoad(ResultScanner scanner, Result result, String expctedVal)
      throws IOException {
    while (result != null) {
      List<Cell> cells = result.getColumnCells(Bytes.toBytes("col"), Bytes.toBytes("q"));
      for (Cell _c : cells) {
        if (Bytes.toString(_c.getRowArray(), _c.getRowOffset(), _c.getRowLength())
            .equals("row1")) {
          System.out
              .println(Bytes.toString(_c.getRowArray(), _c.getRowOffset(), _c.getRowLength()));
          System.out.println(Bytes.toString(_c.getQualifierArray(), _c.getQualifierOffset(),
            _c.getQualifierLength()));
          System.out.println(
            Bytes.toString(_c.getValueArray(), _c.getValueOffset(), _c.getValueLength()));
          Assert.assertEquals(expctedVal,
            Bytes.toString(_c.getValueArray(), _c.getValueOffset(), _c.getValueLength()));
        }
      }
      result = scanner.next();
    }
    return result;
  }

  // If nativeHFile is true, we will set cell seq id and MAX_SEQ_ID_KEY in the file.
  // Else, we will set BULKLOAD_TIME_KEY.
  private Path writeToHFile(long l, String hFilePath, String pathStr, boolean nativeHFile)
      throws IOException {
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    final Path hfilePath = new Path(hFilePath);
    fs.mkdirs(hfilePath);
    Path path = new Path(pathStr);
    HFile.WriterFactory wf = HFile.getWriterFactoryNoCache(TEST_UTIL.getConfiguration());
    Assert.assertNotNull(wf);
    HFileContext context = new HFileContext();
    HFile.Writer writer = wf.withPath(fs, path).withFileContext(context).create();
    KeyValue kv = new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("col"), Bytes.toBytes("q"), l,
        Bytes.toBytes("version2"));

    // Set cell seq id to test bulk load native hfiles.
    if (nativeHFile) {
      // Set a big seq id. Scan should not look at this seq id in a bulk loaded file.
      // Scan should only look at the seq id appended at the bulk load time, and not skip
      // this kv.
      kv.setSequenceId(9999999);
    }

    writer.append(kv);

    if (nativeHFile) {
      // Set a big MAX_SEQ_ID_KEY. Scan should not look at this seq id in a bulk loaded file.
      // Scan should only look at the seq id appended at the bulk load time, and not skip its
      // kv.
      writer.appendFileInfo(MAX_SEQ_ID_KEY, Bytes.toBytes(new Long(9999999)));
    }
    else {
    writer.appendFileInfo(BULKLOAD_TIME_KEY, Bytes.toBytes(System.currentTimeMillis()));
    }
    writer.close();
    return hfilePath;
  }

  private Table init(Admin admin, long l, Scan scan, TableName tableName) throws Exception {
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    Put put0 = new Put(Bytes.toBytes("row1"));
    put0.add(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("col"), Bytes.toBytes("q"), l,
        Bytes.toBytes("version0")));
    table.put(put0);
    admin.flush(tableName);
    Put put1 = new Put(Bytes.toBytes("row2"));
    put1.add(new KeyValue(Bytes.toBytes("row2"), Bytes.toBytes("col"), Bytes.toBytes("q"), l, Bytes
        .toBytes("version0")));
    table.put(put1);
    admin.flush(tableName);
    put0 = new Put(Bytes.toBytes("row1"));
    put0.add(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("col"), Bytes.toBytes("q"), l, Bytes
        .toBytes("version1")));
    table.put(put0);
    admin.flush(tableName);
    admin.compact(tableName);

    ResultScanner scanner = table.getScanner(scan);
    Result result = scanner.next();
    List<Cell> cells = result.getColumnCells(Bytes.toBytes("col"), Bytes.toBytes("q"));
    Assert.assertEquals(1, cells.size());
    Cell _c = cells.get(0);
    Assert.assertEquals("version1",
      Bytes.toString(_c.getValueArray(), _c.getValueOffset(), _c.getValueLength()));
    scanner.close();
    return table;
  }

  @Test
  public void testBulkLoadWithParallelScan() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
      final long l = System.currentTimeMillis();
    final Admin admin = TEST_UTIL.getAdmin();
    createTable(admin, tableName);
    Scan scan = createScan();
    scan.setCaching(1);
    final Table table = init(admin, l, scan, tableName);
    // use bulkload
    final Path hfilePath = writeToHFile(l, "/temp/testBulkLoadWithParallelScan/",
        "/temp/testBulkLoadWithParallelScan/col/file", false);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.mapreduce.bulkload.assign.sequenceNumbers", true);
    final BulkLoadHFiles bulkload = BulkLoadHFiles.create(conf);
    ResultScanner scanner = table.getScanner(scan);
    Result result = scanner.next();
    // Create a scanner and then do bulk load
    final CountDownLatch latch = new CountDownLatch(1);
    new Thread() {
      @Override
      public void run() {
        try {
          Put put1 = new Put(Bytes.toBytes("row5"));
          put1.add(new KeyValue(Bytes.toBytes("row5"), Bytes.toBytes("col"), Bytes.toBytes("q"), l,
              Bytes.toBytes("version0")));
          table.put(put1);
          bulkload.bulkLoad(tableName, hfilePath);
          latch.countDown();
        } catch (TableNotFoundException e) {
        } catch (IOException e) {
        }
      }
    }.start();
    latch.await();
    // By the time we do next() the bulk loaded files are also added to the kv
    // scanner
    scanAfterBulkLoad(scanner, result, "version1");
    scanner.close();
    table.close();
  }

  @Test
  public void testBulkLoadNativeHFile() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    long l = System.currentTimeMillis();
    Admin admin = TEST_UTIL.getAdmin();
    createTable(admin, tableName);
    Scan scan = createScan();
    final Table table = init(admin, l, scan, tableName);
    // use bulkload
    final Path hfilePath = writeToHFile(l, "/temp/testBulkLoadNativeHFile/",
      "/temp/testBulkLoadNativeHFile/col/file", true);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.mapreduce.bulkload.assign.sequenceNumbers", true);
    BulkLoadHFiles.create(conf).bulkLoad(tableName, hfilePath);
    ResultScanner scanner = table.getScanner(scan);
    Result result = scanner.next();
    // We had 'version0', 'version1' for 'row1,col:q' in the table.
    // Bulk load added 'version2'  scanner should be able to see 'version2'
    result = scanAfterBulkLoad(scanner, result, "version2");
    Put put0 = new Put(Bytes.toBytes("row1"));
    put0.add(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("col"), Bytes.toBytes("q"), l, Bytes
        .toBytes("version3")));
    table.put(put0);
    admin.flush(tableName);
    scanner = table.getScanner(scan);
    result = scanner.next();
    while (result != null) {
      List<Cell> cells = result.getColumnCells(Bytes.toBytes("col"), Bytes.toBytes("q"));
      for (Cell _c : cells) {
        if (Bytes.toString(_c.getRowArray(), _c.getRowOffset(), _c.getRowLength())
            .equals("row1")) {
          System.out
              .println(Bytes.toString(_c.getRowArray(), _c.getRowOffset(), _c.getRowLength()));
          System.out.println(Bytes.toString(_c.getQualifierArray(), _c.getQualifierOffset(),
            _c.getQualifierLength()));
          System.out.println(
            Bytes.toString(_c.getValueArray(), _c.getValueOffset(), _c.getValueLength()));
          Assert.assertEquals("version3",
            Bytes.toString(_c.getValueArray(), _c.getValueOffset(), _c.getValueLength()));
        }
      }
      result = scanner.next();
    }
    scanner.close();
    table.close();
  }

  private Scan createScan() {
    Scan scan = new Scan();
    scan.setMaxVersions(3);
    return scan;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
}
