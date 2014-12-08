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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestScannerWithBulkload {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  private static void createTable(HBaseAdmin admin, String tableName) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    hcd.setMaxVersions(3);
    desc.addFamily(hcd);
    admin.createTable(desc);
  }

  @Test
  public void testBulkLoad() throws Exception {
    String tableName = "testBulkLoad";
    long l = System.currentTimeMillis();
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    createTable(admin, tableName);
    Scan scan = createScan();
    final HTable table = init(admin, l, scan, tableName);
    // use bulkload
    final Path hfilePath = writeToHFile(l, "/temp/testBulkLoad/", "/temp/testBulkLoad/col/file",
      false);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.mapreduce.bulkload.assign.sequenceNumbers", true);
    final LoadIncrementalHFiles bulkload = new LoadIncrementalHFiles(conf);
    bulkload.doBulkLoad(hfilePath, table);
    ResultScanner scanner = table.getScanner(scan);
    Result result = scanner.next();
    result = scanAfterBulkLoad(scanner, result, "version2");
    Put put0 = new Put(Bytes.toBytes("row1"));
    put0.add(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("col"), Bytes.toBytes("q"), l, Bytes
        .toBytes("version3")));
    table.put(put0);
    table.flushCommits();
    admin.flush(tableName);
    scanner = table.getScanner(scan);
    result = scanner.next();
    while (result != null) {
      List<KeyValue> kvs = result.getColumn(Bytes.toBytes("col"), Bytes.toBytes("q"));
      for (KeyValue _kv : kvs) {
        if (Bytes.toString(_kv.getRow()).equals("row1")) {
          System.out.println(Bytes.toString(_kv.getRow()));
          System.out.println(Bytes.toString(_kv.getQualifier()));
          System.out.println(Bytes.toString(_kv.getValue()));
          Assert.assertEquals("version3", Bytes.toString(_kv.getValue()));
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
      List<KeyValue> kvs = result.getColumn(Bytes.toBytes("col"), Bytes.toBytes("q"));
      for (KeyValue _kv : kvs) {
        if (Bytes.toString(_kv.getRow()).equals("row1")) {
          System.out.println(Bytes.toString(_kv.getRow()));
          System.out.println(Bytes.toString(_kv.getQualifier()));
          System.out.println(Bytes.toString(_kv.getValue()));
          Assert.assertEquals(expctedVal, Bytes.toString(_kv.getValue()));
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

    // Set cell mvcc to test bulk load native hfiles.
    if (nativeHFile) {
      // Set a big seq id. Scan should not look at this seq id in a bulk loaded file.
      // Scan should only look at the seq id appended at the bulk load time, and not skip
      // this kv.
      kv.setMvccVersion(9999999);
    }

    writer.append(kv);

    if (nativeHFile) {
      // Set a big MAX_SEQ_ID_KEY. Scan should not look at this seq id in a bulk loaded file.
      // Scan should only look at the seq id appended at the bulk load time, and not skip its
      // kv.
      writer.appendFileInfo(StoreFile.MAX_SEQ_ID_KEY, Bytes.toBytes(new Long(9999999)));
    }
    else {
    writer.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY, Bytes.toBytes(System.currentTimeMillis()));
    }
    writer.close();
    return hfilePath;
  }

  private HTable init(HBaseAdmin admin, long l, Scan scan, String tableName) throws Exception {
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Put put0 = new Put(Bytes.toBytes("row1"));
    put0.add(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("col"), Bytes.toBytes("q"), l, Bytes
        .toBytes("version0")));
    table.put(put0);
    table.flushCommits();
    admin.flush(tableName);
    Put put1 = new Put(Bytes.toBytes("row2"));
    put1.add(new KeyValue(Bytes.toBytes("row2"), Bytes.toBytes("col"), Bytes.toBytes("q"), l, Bytes
        .toBytes("version0")));
    table.put(put1);
    table.flushCommits();
    admin.flush(tableName);
    admin.close();
    put0 = new Put(Bytes.toBytes("row1"));
    put0.add(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("col"), Bytes.toBytes("q"), l, Bytes
        .toBytes("version1")));
    table.put(put0);
    table.flushCommits();
    admin.flush(tableName);
    admin.compact(tableName);

    ResultScanner scanner = table.getScanner(scan);
    Result result = scanner.next();
    List<KeyValue> kvs = result.getColumn(Bytes.toBytes("col"), Bytes.toBytes("q"));
    Assert.assertEquals(1, kvs.size());
    Assert.assertEquals("version1", Bytes.toString(kvs.get(0).getValue()));
    scanner.close();
    return table;
  }

  @Test
  public void testBulkLoadWithParallelScan() throws Exception {
    String tableName = "testBulkLoadWithParallelScan";
    final long l = System.currentTimeMillis();
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    createTable(admin, tableName);
    Scan scan = createScan();
    final HTable table = init(admin, l, scan, tableName);
    // use bulkload
    final Path hfilePath = writeToHFile(l, "/temp/testBulkLoadWithParallelScan/",
        "/temp/testBulkLoadWithParallelScan/col/file", false);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.mapreduce.bulkload.assign.sequenceNumbers", true);
    final LoadIncrementalHFiles bulkload = new LoadIncrementalHFiles(conf);
    ResultScanner scanner = table.getScanner(scan);
    // Create a scanner and then do bulk load
    final CountDownLatch latch = new CountDownLatch(1);
    new Thread() {
      public void run() {
        try {
          Put put1 = new Put(Bytes.toBytes("row5"));
          put1.add(new KeyValue(Bytes.toBytes("row5"), Bytes.toBytes("col"), Bytes.toBytes("q"), l,
              Bytes.toBytes("version0")));
          table.put(put1);
          table.flushCommits();
          bulkload.doBulkLoad(hfilePath, table);
          latch.countDown();
        } catch (TableNotFoundException e) {
        } catch (IOException e) {
        }
      }
    }.start();
    latch.await();
    // We had 'version0', 'version1' for row1,col:q. Bulk load adds 'version2'
    // By the time we do next() the bulk loaded file is also added to the kv
    // scanner and is immediately visible with no mvcc check.
    Result result = scanner.next();
    scanAfterBulkLoad(scanner, result, "version2");
    scanner.close();
    table.close();

  }

  @Test
  public void testBulkLoadNativeHFile() throws Exception {
    String tableName = "testBulkLoadNativeHFile";
    long l = System.currentTimeMillis();
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    createTable(admin, tableName);
    Scan scan = createScan();
    final HTable table = init(admin, l, scan, tableName);
    // use bulkload
    final Path hfilePath = writeToHFile(l, "/temp/testBulkLoadNativeHFile/",
      "/temp/testBulkLoadNativeHFile/col/file", true);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.mapreduce.bulkload.assign.sequenceNumbers", true);
    final LoadIncrementalHFiles bulkload = new LoadIncrementalHFiles(conf);
    bulkload.doBulkLoad(hfilePath, table);
    ResultScanner scanner = table.getScanner(scan);
    Result result = scanner.next();
    // We had 'version0', 'version1' for 'row1,col:q' in the table.
    // Bulk load added 'version2'  scanner should be able to see 'version2'
    result = scanAfterBulkLoad(scanner, result, "version2");
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
