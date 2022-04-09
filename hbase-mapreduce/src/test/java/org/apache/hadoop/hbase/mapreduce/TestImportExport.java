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
package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.hbase.HConstants.RPC_CODEC_CONF_KEY;
import static org.apache.hadoop.hbase.ipc.RpcClient.DEFAULT_CODEC_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.Import.KeyValueImporter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LauncherSecurityManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the table import and table export MR job functionality
 */
@Category({VerySlowMapReduceTests.class, MediumTests.class})
//TODO : Remove this in 3.0
public class TestImportExport {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestImportExport.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestImportExport.class);
  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] ROW1 = Bytes.toBytesBinary("\\x32row1");
  private static final byte[] ROW2 = Bytes.toBytesBinary("\\x32row2");
  private static final byte[] ROW3 = Bytes.toBytesBinary("\\x32row3");
  private static final String FAMILYA_STRING = "a";
  private static final String FAMILYB_STRING = "b";
  private static final byte[] FAMILYA = Bytes.toBytes(FAMILYA_STRING);
  private static final byte[] FAMILYB = Bytes.toBytes(FAMILYB_STRING);
  private static final byte[] QUAL = Bytes.toBytes("q");
  private static final String OUTPUT_DIR = "outputdir";
  private static String FQ_OUTPUT_DIR;
  private static final String EXPORT_BATCH_SIZE = "100";

  private static final long now = System.currentTimeMillis();
  private final TableName EXPORT_TABLE = TableName.valueOf("export_table");
  private final TableName IMPORT_TABLE = TableName.valueOf("import_table");
  public static final byte TEST_TAG_TYPE =  (byte) (Tag.CUSTOM_TAG_TYPE_RANGE + 1);
  public static final String TEST_ATTR = "source_op";
  public static final String TEST_TAG = "test_tag";

  @BeforeClass
  public static void beforeClass() throws Throwable {
    // Up the handlers; this test needs more than usual.
    UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 10);
    UTIL.startMiniCluster();
    FQ_OUTPUT_DIR =
      new Path(OUTPUT_DIR).makeQualified(FileSystem.get(UTIL.getConfiguration())).toString();
  }

  @AfterClass
  public static void afterClass() throws Throwable {
    UTIL.shutdownMiniCluster();
  }

  @Rule
  public final TestName name = new TestName();

  @Before
  public void announce() {
    LOG.info("Running " + name.getMethodName());
  }

  @After
  public void cleanup() throws Throwable {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    fs.delete(new Path(OUTPUT_DIR), true);
    if (UTIL.getAdmin().tableExists(EXPORT_TABLE)) {
      UTIL.deleteTable(EXPORT_TABLE);
    }
    if (UTIL.getAdmin().tableExists(IMPORT_TABLE)) {
      UTIL.deleteTable(IMPORT_TABLE);
    }
  }

  /**
   * Runs an export job with the specified command line args
   * @param args
   * @return true if job completed successfully
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  protected boolean runExport(String[] args) throws Throwable {
    // need to make a copy of the configuration because to make sure different temp dirs are used.
    int status = ToolRunner.run(new Configuration(UTIL.getConfiguration()), new Export(), args);
    return status == 0;
  }

  protected void runExportMain(String[] args) throws Throwable {
    Export.main(args);
  }

  /**
   * Runs an import job with the specified command line args
   * @param args
   * @return true if job completed successfully
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  boolean runImport(String[] args) throws Throwable {
    // need to make a copy of the configuration because to make sure different temp dirs are used.
    int status = ToolRunner.run(new Configuration(UTIL.getConfiguration()), new Import(), args);
    return status == 0;
  }

  /**
   * Test simple replication case with column mapping
   * @throws Exception
   */
  @Test
  public void testSimpleCase() throws Throwable {
    try (Table t = UTIL.createTable(TableName.valueOf(name.getMethodName()), FAMILYA, 3);) {
      Put p = new Put(ROW1);
      p.addColumn(FAMILYA, QUAL, now, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 1, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 2, QUAL);
      t.put(p);
      p = new Put(ROW2);
      p.addColumn(FAMILYA, QUAL, now, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 1, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 2, QUAL);
      t.put(p);
      p = new Put(ROW3);
      p.addColumn(FAMILYA, QUAL, now, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 1, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 2, QUAL);
      t.put(p);
    }

      String[] args = new String[] {
          // Only export row1 & row2.
          "-D" + TableInputFormat.SCAN_ROW_START + "=\\x32row1",
          "-D" + TableInputFormat.SCAN_ROW_STOP + "=\\x32row3",
          name.getMethodName(),
          FQ_OUTPUT_DIR,
          "1000", // max number of key versions per key to export
      };
      assertTrue(runExport(args));

      final String IMPORT_TABLE = name.getMethodName() + "import";
      try (Table t = UTIL.createTable(TableName.valueOf(IMPORT_TABLE), FAMILYB, 3);) {
        args = new String[] {
            "-D" + Import.CF_RENAME_PROP + "="+FAMILYA_STRING+":"+FAMILYB_STRING,
            IMPORT_TABLE,
            FQ_OUTPUT_DIR
        };
        assertTrue(runImport(args));

        Get g = new Get(ROW1);
        g.setMaxVersions();
        Result r = t.get(g);
        assertEquals(3, r.size());
        g = new Get(ROW2);
        g.setMaxVersions();
        r = t.get(g);
        assertEquals(3, r.size());
        g = new Get(ROW3);
        r = t.get(g);
        assertEquals(0, r.size());
      }
  }

  /**
   * Test export hbase:meta table
   *
   * @throws Throwable
   */
  @Test
  public void testMetaExport() throws Throwable {
    String[] args = new String[] { TableName.META_TABLE_NAME.getNameAsString(),
      FQ_OUTPUT_DIR, "1", "0", "0" };
    assertTrue(runExport(args));
  }

  /**
   * Test import data from 0.94 exported file
   * @throws Throwable
   */
  @Test
  public void testImport94Table() throws Throwable {
    final String name = "exportedTableIn94Format";
    URL url = TestImportExport.class.getResource(name);
    File f = new File(url.toURI());
    if (!f.exists()) {
      LOG.warn("FAILED TO FIND " + f + "; skipping out on test");
      return;
    }
    assertTrue(f.exists());
    LOG.info("FILE=" + f);
    Path importPath = new Path(f.toURI());
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    fs.copyFromLocalFile(importPath, new Path(FQ_OUTPUT_DIR + Path.SEPARATOR + name));
    String IMPORT_TABLE = name;
    try (Table t = UTIL.createTable(TableName.valueOf(IMPORT_TABLE), Bytes.toBytes("f1"), 3);) {
      String[] args = new String[] {
              "-Dhbase.import.version=0.94" ,
              IMPORT_TABLE, FQ_OUTPUT_DIR
      };
      assertTrue(runImport(args));
      // @formatter:off
      // exportedTableIn94Format contains 5 rows
      // ROW         COLUMN+CELL
      // r1          column=f1:c1, timestamp=1383766761171, value=val1
      // r2          column=f1:c1, timestamp=1383766771642, value=val2
      // r3          column=f1:c1, timestamp=1383766777615, value=val3
      // r4          column=f1:c1, timestamp=1383766785146, value=val4
      // r5          column=f1:c1, timestamp=1383766791506, value=val5
      // @formatter:on
      assertEquals(5, UTIL.countRows(t));
    }
  }

  /**
   * Test export scanner batching
   */
   @Test
   public void testExportScannerBatching() throws Throwable {
    TableDescriptor desc = TableDescriptorBuilder
            .newBuilder(TableName.valueOf(name.getMethodName()))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILYA)
              .setMaxVersions(1)
              .build())
            .build();
    UTIL.getAdmin().createTable(desc);
    try (Table t = UTIL.getConnection().getTable(desc.getTableName());) {

      Put p = new Put(ROW1);
      p.addColumn(FAMILYA, QUAL, now, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 1, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 2, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 3, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 4, QUAL);
      t.put(p);
      // added scanner batching arg.
      String[] args = new String[] { "-D" + ExportUtils.EXPORT_BATCHING + "=" + EXPORT_BATCH_SIZE,
        name.getMethodName(), FQ_OUTPUT_DIR };
      assertTrue(runExport(args));

      FileSystem fs = FileSystem.get(UTIL.getConfiguration());
      fs.delete(new Path(FQ_OUTPUT_DIR), true);
    }
  }

  @Test
  public void testWithDeletes() throws Throwable {
    TableDescriptor desc = TableDescriptorBuilder
            .newBuilder(TableName.valueOf(name.getMethodName()))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILYA)
              .setMaxVersions(5)
              .setKeepDeletedCells(KeepDeletedCells.TRUE)
              .build())
            .build();
    UTIL.getAdmin().createTable(desc);
    try (Table t = UTIL.getConnection().getTable(desc.getTableName());) {

      Put p = new Put(ROW1);
      p.addColumn(FAMILYA, QUAL, now, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 1, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 2, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 3, QUAL);
      p.addColumn(FAMILYA, QUAL, now + 4, QUAL);
      t.put(p);

      Delete d = new Delete(ROW1, now+3);
      t.delete(d);
      d = new Delete(ROW1);
      d.addColumns(FAMILYA, QUAL, now+2);
      t.delete(d);
    }

    String[] args = new String[] {
        "-D" + ExportUtils.RAW_SCAN + "=true",
        name.getMethodName(),
        FQ_OUTPUT_DIR,
        "1000", // max number of key versions per key to export
    };
    assertTrue(runExport(args));

    final String IMPORT_TABLE = name.getMethodName() + "import";
    desc = TableDescriptorBuilder
            .newBuilder(TableName.valueOf(IMPORT_TABLE))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILYA)
              .setMaxVersions(5)
              .setKeepDeletedCells(KeepDeletedCells.TRUE)
              .build())
            .build();
    UTIL.getAdmin().createTable(desc);
    try (Table t = UTIL.getConnection().getTable(desc.getTableName());) {
      args = new String[] {
          IMPORT_TABLE,
          FQ_OUTPUT_DIR
      };
      assertTrue(runImport(args));

      Scan s = new Scan();
      s.setMaxVersions();
      s.setRaw(true);
      ResultScanner scanner = t.getScanner(s);
      Result r = scanner.next();
      Cell[] res = r.rawCells();
      assertTrue(PrivateCellUtil.isDeleteFamily(res[0]));
      assertEquals(now+4, res[1].getTimestamp());
      assertEquals(now+3, res[2].getTimestamp());
      assertTrue(CellUtil.isDelete(res[3]));
      assertEquals(now+2, res[4].getTimestamp());
      assertEquals(now+1, res[5].getTimestamp());
      assertEquals(now, res[6].getTimestamp());
    }
  }


  @Test
  public void testWithMultipleDeleteFamilyMarkersOfSameRowSameFamily() throws Throwable {
    final TableName exportTable = TableName.valueOf(name.getMethodName());
    TableDescriptor desc = TableDescriptorBuilder
            .newBuilder(TableName.valueOf(name.getMethodName()))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILYA)
              .setMaxVersions(5)
              .setKeepDeletedCells(KeepDeletedCells.TRUE)
              .build())
            .build();
    UTIL.getAdmin().createTable(desc);

    Table exportT = UTIL.getConnection().getTable(exportTable);

    //Add first version of QUAL
    Put p = new Put(ROW1);
    p.addColumn(FAMILYA, QUAL, now, QUAL);
    exportT.put(p);

    //Add Delete family marker
    Delete d = new Delete(ROW1, now+3);
    exportT.delete(d);

    //Add second version of QUAL
    p = new Put(ROW1);
    p.addColumn(FAMILYA, QUAL, now + 5, "s".getBytes());
    exportT.put(p);

    //Add second Delete family marker
    d = new Delete(ROW1, now+7);
    exportT.delete(d);


    String[] args = new String[] {
        "-D" + ExportUtils.RAW_SCAN + "=true", exportTable.getNameAsString(),
        FQ_OUTPUT_DIR,
        "1000", // max number of key versions per key to export
    };
    assertTrue(runExport(args));

    final String importTable = name.getMethodName() + "import";
    desc = TableDescriptorBuilder
            .newBuilder(TableName.valueOf(importTable))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILYA)
              .setMaxVersions(5)
              .setKeepDeletedCells(KeepDeletedCells.TRUE)
              .build())
            .build();
    UTIL.getAdmin().createTable(desc);

    Table importT = UTIL.getConnection().getTable(TableName.valueOf(importTable));
    args = new String[] {
        importTable,
        FQ_OUTPUT_DIR
    };
    assertTrue(runImport(args));

    Scan s = new Scan();
    s.setMaxVersions();
    s.setRaw(true);

    ResultScanner importedTScanner = importT.getScanner(s);
    Result importedTResult = importedTScanner.next();

    ResultScanner exportedTScanner = exportT.getScanner(s);
    Result  exportedTResult =  exportedTScanner.next();
    try {
      Result.compareResults(exportedTResult, importedTResult);
    } catch (Throwable e) {
      fail("Original and imported tables data comparision failed with error:"+e.getMessage());
    } finally {
      exportT.close();
      importT.close();
    }
  }

  /**
   * Create a simple table, run an Export Job on it, Import with filtering on,  verify counts,
   * attempt with invalid values.
   */
  @Test
  public void testWithFilter() throws Throwable {
    // Create simple table to export
    TableDescriptor desc = TableDescriptorBuilder
            .newBuilder(TableName.valueOf(name.getMethodName()))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILYA)
              .setMaxVersions(5)
              .build())
            .build();
    UTIL.getAdmin().createTable(desc);
    Table exportTable = UTIL.getConnection().getTable(desc.getTableName());

    Put p1 = new Put(ROW1);
    p1.addColumn(FAMILYA, QUAL, now, QUAL);
    p1.addColumn(FAMILYA, QUAL, now + 1, QUAL);
    p1.addColumn(FAMILYA, QUAL, now + 2, QUAL);
    p1.addColumn(FAMILYA, QUAL, now + 3, QUAL);
    p1.addColumn(FAMILYA, QUAL, now + 4, QUAL);

    // Having another row would actually test the filter.
    Put p2 = new Put(ROW2);
    p2.addColumn(FAMILYA, QUAL, now, QUAL);

    exportTable.put(Arrays.asList(p1, p2));

    // Export the simple table
    String[] args = new String[] { name.getMethodName(), FQ_OUTPUT_DIR, "1000" };
    assertTrue(runExport(args));

    // Import to a new table
    final String IMPORT_TABLE = name.getMethodName() + "import";
    desc = TableDescriptorBuilder
            .newBuilder(TableName.valueOf(IMPORT_TABLE))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILYA)
              .setMaxVersions(5)
              .build())
            .build();
    UTIL.getAdmin().createTable(desc);

    Table importTable = UTIL.getConnection().getTable(desc.getTableName());
    args = new String[] { "-D" + Import.FILTER_CLASS_CONF_KEY + "=" + PrefixFilter.class.getName(),
        "-D" + Import.FILTER_ARGS_CONF_KEY + "=" + Bytes.toString(ROW1), IMPORT_TABLE,
        FQ_OUTPUT_DIR,
        "1000" };
    assertTrue(runImport(args));

    // get the count of the source table for that time range
    PrefixFilter filter = new PrefixFilter(ROW1);
    int count = getCount(exportTable, filter);

    Assert.assertEquals("Unexpected row count between export and import tables", count,
      getCount(importTable, null));

    // and then test that a broken command doesn't bork everything - easier here because we don't
    // need to re-run the export job

    args = new String[] { "-D" + Import.FILTER_CLASS_CONF_KEY + "=" + Filter.class.getName(),
        "-D" + Import.FILTER_ARGS_CONF_KEY + "=" + Bytes.toString(ROW1) + "", name.getMethodName(),
        FQ_OUTPUT_DIR, "1000" };
    assertFalse(runImport(args));

    // cleanup
    exportTable.close();
    importTable.close();
  }

  /**
   * Count the number of keyvalues in the specified table with the given filter
   * @param table the table to scan
   * @return the number of keyvalues found
   * @throws IOException
   */
  private int getCount(Table table, Filter filter) throws IOException {
    Scan scan = new Scan();
    scan.setFilter(filter);
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count += res.size();
    }
    results.close();
    return count;
  }

  /**
   * test main method. Import should print help and call System.exit
   */
  @Test
  public void testImportMain() throws Throwable {
    PrintStream oldPrintStream = System.err;
    SecurityManager SECURITY_MANAGER = System.getSecurityManager();
    LauncherSecurityManager newSecurityManager= new LauncherSecurityManager();
    System.setSecurityManager(newSecurityManager);
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    String[] args = {};
    System.setErr(new PrintStream(data));
    try {
      System.setErr(new PrintStream(data));
      Import.main(args);
      fail("should be SecurityException");
    } catch (SecurityException e) {
      assertEquals(-1, newSecurityManager.getExitCode());
      assertTrue(data.toString().contains("Wrong number of arguments:"));
      assertTrue(data.toString().contains("-Dimport.bulk.output=/path/for/output"));
      assertTrue(data.toString().contains("-Dimport.filter.class=<name of filter class>"));
      assertTrue(data.toString().contains("-Dimport.bulk.output=/path/for/output"));
      assertTrue(data.toString().contains("-Dmapreduce.reduce.speculative=false"));
    } finally {
      System.setErr(oldPrintStream);
      System.setSecurityManager(SECURITY_MANAGER);
    }
  }

  @Test
  public void testExportScan() throws Exception {
    int version = 100;
    long startTime = System.currentTimeMillis();
    long endTime = startTime + 1;
    String prefix = "row";
    String label_0 = "label_0";
    String label_1 = "label_1";
    String[] args = {
      "table",
      "outputDir",
      String.valueOf(version),
      String.valueOf(startTime),
      String.valueOf(endTime),
      prefix
    };
    Scan scan = ExportUtils.getScanFromCommandLine(UTIL.getConfiguration(), args);
    assertEquals(version, scan.getMaxVersions());
    assertEquals(startTime, scan.getTimeRange().getMin());
    assertEquals(endTime, scan.getTimeRange().getMax());
    assertEquals(true, (scan.getFilter() instanceof PrefixFilter));
    assertEquals(0, Bytes.compareTo(((PrefixFilter) scan.getFilter()).getPrefix(), Bytes.toBytesBinary(prefix)));
    String[] argsWithLabels = {
      "-D " + ExportUtils.EXPORT_VISIBILITY_LABELS + "=" + label_0 + "," + label_1,
      "table",
      "outputDir",
      String.valueOf(version),
      String.valueOf(startTime),
      String.valueOf(endTime),
      prefix
    };
    Configuration conf = new Configuration(UTIL.getConfiguration());
    // parse the "-D" options
    String[] otherArgs = new GenericOptionsParser(conf, argsWithLabels).getRemainingArgs();
    Scan scanWithLabels = ExportUtils.getScanFromCommandLine(conf, otherArgs);
    assertEquals(version, scanWithLabels.getMaxVersions());
    assertEquals(startTime, scanWithLabels.getTimeRange().getMin());
    assertEquals(endTime, scanWithLabels.getTimeRange().getMax());
    assertEquals(true, (scanWithLabels.getFilter() instanceof PrefixFilter));
    assertEquals(0, Bytes.compareTo(((PrefixFilter) scanWithLabels.getFilter()).getPrefix(), Bytes.toBytesBinary(prefix)));
    assertEquals(2, scanWithLabels.getAuthorizations().getLabels().size());
    assertEquals(label_0, scanWithLabels.getAuthorizations().getLabels().get(0));
    assertEquals(label_1, scanWithLabels.getAuthorizations().getLabels().get(1));
  }

  /**
   * test main method. Export should print help and call System.exit
   */
  @Test
  public void testExportMain() throws Throwable {
    PrintStream oldPrintStream = System.err;
    SecurityManager SECURITY_MANAGER = System.getSecurityManager();
    LauncherSecurityManager newSecurityManager= new LauncherSecurityManager();
    System.setSecurityManager(newSecurityManager);
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    String[] args = {};
    System.setErr(new PrintStream(data));
    try {
      System.setErr(new PrintStream(data));
      runExportMain(args);
      fail("should be SecurityException");
    } catch (SecurityException e) {
      assertEquals(-1, newSecurityManager.getExitCode());
      String errMsg = data.toString();
      assertTrue(errMsg.contains("Wrong number of arguments:"));
      assertTrue(errMsg.contains(
              "Usage: Export [-D <property=value>]* <tablename> <outputdir> [<versions> " +
              "[<starttime> [<endtime>]] [^[regex pattern] or [Prefix] to filter]]"));
      assertTrue(
        errMsg.contains("-D hbase.mapreduce.scan.column.family=<family1>,<family2>, ..."));
      assertTrue(errMsg.contains("-D hbase.mapreduce.include.deleted.rows=true"));
      assertTrue(errMsg.contains("-D hbase.client.scanner.caching=100"));
      assertTrue(errMsg.contains("-D hbase.export.scanner.batch=10"));
      assertTrue(errMsg.contains("-D hbase.export.scanner.caching=100"));
    } finally {
      System.setErr(oldPrintStream);
      System.setSecurityManager(SECURITY_MANAGER);
    }
  }

  /**
   * Test map method of Importer
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testKeyValueImporter() throws Throwable {
    KeyValueImporter importer = new KeyValueImporter();
    Configuration configuration = new Configuration();
    Context ctx = mock(Context.class);
    when(ctx.getConfiguration()).thenReturn(configuration);

    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ImmutableBytesWritable writer = invocation.getArgument(0);
        KeyValue key = invocation.getArgument(1);
        assertEquals("Key", Bytes.toString(writer.get()));
        assertEquals("row", Bytes.toString(CellUtil.cloneRow(key)));
        return null;
      }
    }).when(ctx).write(any(), any());

    importer.setup(ctx);
    Result value = mock(Result.class);
    KeyValue[] keys = {
        new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("family"), Bytes.toBytes("qualifier"),
            Bytes.toBytes("value")),
        new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("family"), Bytes.toBytes("qualifier"),
            Bytes.toBytes("value1")) };
    when(value.rawCells()).thenReturn(keys);
    importer.map(new ImmutableBytesWritable(Bytes.toBytes("Key")), value, ctx);

  }

  /**
   * Test addFilterAndArguments method of Import This method set couple
   * parameters into Configuration
   */
  @Test
  public void testAddFilterAndArguments() throws IOException {
    Configuration configuration = new Configuration();

    List<String> args = new ArrayList<>();
    args.add("param1");
    args.add("param2");

    Import.addFilterAndArguments(configuration, FilterBase.class, args);
    assertEquals("org.apache.hadoop.hbase.filter.FilterBase",
        configuration.get(Import.FILTER_CLASS_CONF_KEY));
    assertEquals("param1,param2", configuration.get(Import.FILTER_ARGS_CONF_KEY));
  }

  @Test
  public void testDurability() throws Throwable {
    // Create an export table.
    String exportTableName = name.getMethodName() + "export";
    try (Table exportTable = UTIL.createTable(TableName.valueOf(exportTableName), FAMILYA, 3);) {

      // Insert some data
      Put put = new Put(ROW1);
      put.addColumn(FAMILYA, QUAL, now, QUAL);
      put.addColumn(FAMILYA, QUAL, now + 1, QUAL);
      put.addColumn(FAMILYA, QUAL, now + 2, QUAL);
      exportTable.put(put);

      put = new Put(ROW2);
      put.addColumn(FAMILYA, QUAL, now, QUAL);
      put.addColumn(FAMILYA, QUAL, now + 1, QUAL);
      put.addColumn(FAMILYA, QUAL, now + 2, QUAL);
      exportTable.put(put);

      // Run the export
      String[] args = new String[] { exportTableName, FQ_OUTPUT_DIR, "1000"};
      assertTrue(runExport(args));

      // Create the table for import
      String importTableName = name.getMethodName() + "import1";
      Table importTable = UTIL.createTable(TableName.valueOf(importTableName), FAMILYA, 3);

      // Register the wal listener for the import table
      RegionInfo region = UTIL.getHBaseCluster().getRegionServerThreads().get(0).getRegionServer()
          .getRegions(importTable.getName()).get(0).getRegionInfo();
      TableWALActionListener walListener = new TableWALActionListener(region);
      WAL wal = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL(region);
      wal.registerWALActionsListener(walListener);

      // Run the import with SKIP_WAL
      args =
          new String[] { "-D" + Import.WAL_DURABILITY + "=" + Durability.SKIP_WAL.name(),
              importTableName, FQ_OUTPUT_DIR };
      assertTrue(runImport(args));
      //Assert that the wal is not visisted
      assertTrue(!walListener.isWALVisited());
      //Ensure that the count is 2 (only one version of key value is obtained)
      assertTrue(getCount(importTable, null) == 2);

      // Run the import with the default durability option
      importTableName = name.getMethodName() + "import2";
      importTable = UTIL.createTable(TableName.valueOf(importTableName), FAMILYA, 3);
      region = UTIL.getHBaseCluster().getRegionServerThreads().get(0).getRegionServer()
          .getRegions(importTable.getName()).get(0).getRegionInfo();
      wal = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL(region);
      walListener = new TableWALActionListener(region);
      wal.registerWALActionsListener(walListener);
      args = new String[] { importTableName, FQ_OUTPUT_DIR };
      assertTrue(runImport(args));
      //Assert that the wal is visisted
      assertTrue(walListener.isWALVisited());
      //Ensure that the count is 2 (only one version of key value is obtained)
      assertTrue(getCount(importTable, null) == 2);
    }
  }

  /**
   * This listens to the {@link #visitLogEntryBeforeWrite(RegionInfo, WALKey, WALEdit)} to
   * identify that an entry is written to the Write Ahead Log for the given table.
   */
  private static class TableWALActionListener implements WALActionsListener {

    private RegionInfo regionInfo;
    private boolean isVisited = false;

    public TableWALActionListener(RegionInfo region) {
      this.regionInfo = region;
    }

    @Override
    public void visitLogEntryBeforeWrite(WALKey logKey, WALEdit logEdit) {
      if (logKey.getTableName().getNameAsString().equalsIgnoreCase(
          this.regionInfo.getTable().getNameAsString()) && (!logEdit.isMetaEdit())) {
        isVisited = true;
      }
    }

    public boolean isWALVisited() {
      return isVisited;
    }
  }

  /**
   *  Add cell tags to delete mutations, run export and import tool and
   *  verify that tags are present in import table also.
   * @throws Throwable throws Throwable.
   */
  @Test
  public void testTagsAddition() throws Throwable {
    final TableName exportTable = TableName.valueOf(name.getMethodName());
    TableDescriptor desc = TableDescriptorBuilder
      .newBuilder(exportTable)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILYA)
        .setMaxVersions(5)
        .setKeepDeletedCells(KeepDeletedCells.TRUE)
        .build())
      .setCoprocessor(MetadataController.class.getName())
      .build();
    UTIL.getAdmin().createTable(desc);

    Table exportT = UTIL.getConnection().getTable(exportTable);

    //Add first version of QUAL
    Put p = new Put(ROW1);
    p.addColumn(FAMILYA, QUAL, now, QUAL);
    exportT.put(p);

    //Add Delete family marker
    Delete d = new Delete(ROW1, now+3);
    // Add test attribute to delete mutation.
    d.setAttribute(TEST_ATTR, Bytes.toBytes(TEST_TAG));
    exportT.delete(d);

    // Run export tool with KeyValueCodecWithTags as Codec. This will ensure that export tool
    // will use KeyValueCodecWithTags.
    String[] args = new String[] {
      "-D" + ExportUtils.RAW_SCAN + "=true",
      // This will make sure that codec will encode and decode tags in rpc call.
      "-Dhbase.client.rpc.codec=org.apache.hadoop.hbase.codec.KeyValueCodecWithTags",
      exportTable.getNameAsString(),
      FQ_OUTPUT_DIR,
      "1000", // max number of key versions per key to export
    };
    assertTrue(runExport(args));
    // Assert tag exists in exportTable
    checkWhetherTagExists(exportTable, true);

    // Create an import table with MetadataController.
    final TableName importTable = TableName.valueOf("importWithTestTagsAddition");
    TableDescriptor importTableDesc = TableDescriptorBuilder
      .newBuilder(importTable)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILYA)
        .setMaxVersions(5)
        .setKeepDeletedCells(KeepDeletedCells.TRUE)
        .build())
      .setCoprocessor(MetadataController.class.getName())
      .build();
    UTIL.getAdmin().createTable(importTableDesc);

    // Run import tool.
    args = new String[] {
      // This will make sure that codec will encode and decode tags in rpc call.
      "-Dhbase.client.rpc.codec=org.apache.hadoop.hbase.codec.KeyValueCodecWithTags",
      importTable.getNameAsString(),
      FQ_OUTPUT_DIR
    };
    assertTrue(runImport(args));
    // Make sure that tags exists in imported table.
    checkWhetherTagExists(importTable, true);
  }

  private void checkWhetherTagExists(TableName table, boolean tagExists) throws IOException {
    List<Cell> values = new ArrayList<>();
    for (HRegion region : UTIL.getHBaseCluster().getRegions(table)) {
      Scan scan = new Scan();
      // Make sure to set rawScan to true so that we will get Delete Markers.
      scan.setRaw(true);
      scan.readAllVersions();
      scan.withStartRow(ROW1);
      // Need to use RegionScanner instead of table#getScanner since the latter will
      // not return tags since it will go through rpc layer and remove tags intentionally.
      RegionScanner scanner = region.getScanner(scan);
      scanner.next(values);
      if (!values.isEmpty()) {
        break;
      }
    }
    boolean deleteFound = false;
    for (Cell cell: values) {
      if (PrivateCellUtil.isDelete(cell.getType().getCode())) {
        deleteFound = true;
        List<Tag> tags = PrivateCellUtil.getTags(cell);
        // If tagExists flag is true then validate whether tag contents are as expected.
        if (tagExists) {
          Assert.assertEquals(1, tags.size());
          for (Tag tag : tags) {
            Assert.assertEquals(TEST_TAG, Tag.getValueAsString(tag));
          }
        } else {
          // If tagExists flag is disabled then check for 0 size tags.
          assertEquals(0, tags.size());
        }
      }
    }
    Assert.assertTrue(deleteFound);
  }

  /*
    This co-proc will add a cell tag to delete mutation.
   */
  public static class MetadataController implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                               MiniBatchOperationInProgress<Mutation> miniBatchOp)
      throws IOException {
      if (c.getEnvironment().getRegion().getRegionInfo().getTable().isSystemTable()) {
        return;
      }
      for (int i = 0; i < miniBatchOp.size(); i++) {
        Mutation m = miniBatchOp.getOperation(i);
        if (!(m instanceof Delete)) {
          continue;
        }
        byte[] sourceOpAttr = m.getAttribute(TEST_ATTR);
        if (sourceOpAttr == null) {
          continue;
        }
        Tag sourceOpTag = new ArrayBackedTag(TEST_TAG_TYPE, sourceOpAttr);
        List<Cell> updatedCells = new ArrayList<>();
        for (CellScanner cellScanner = m.cellScanner(); cellScanner.advance(); ) {
          Cell cell = cellScanner.current();
          List<Tag> tags = PrivateCellUtil.getTags(cell);
          tags.add(sourceOpTag);
          Cell updatedCell = PrivateCellUtil.createCell(cell, tags);
          updatedCells.add(updatedCell);
        }
        m.getFamilyCellMap().clear();
        // Clear and add new Cells to the Mutation.
        for (Cell cell : updatedCells) {
          Delete d = (Delete) m;
          d.add(cell);
        }
      }
    }
  }

  /**
   * Set hbase.client.rpc.codec and hbase.client.default.rpc.codec both to empty string
   * This means it will use no Codec. Make sure that we don't return Tags in response.
   * @throws Exception Exception
   */
  @Test
  public void testTagsWithEmptyCodec() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor tableDesc = TableDescriptorBuilder
      .newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILYA)
        .setMaxVersions(5)
        .setKeepDeletedCells(KeepDeletedCells.TRUE)
        .build())
      .setCoprocessor(MetadataController.class.getName())
      .build();
    UTIL.getAdmin().createTable(tableDesc);
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.set(RPC_CODEC_CONF_KEY, "");
    conf.set(DEFAULT_CODEC_CLASS, "");
    try (Connection connection = ConnectionFactory.createConnection(conf);
         Table table = connection.getTable(tableName)) {
      //Add first version of QUAL
      Put p = new Put(ROW1);
      p.addColumn(FAMILYA, QUAL, now, QUAL);
      table.put(p);

      //Add Delete family marker
      Delete d = new Delete(ROW1, now+3);
      // Add test attribute to delete mutation.
      d.setAttribute(TEST_ATTR, Bytes.toBytes(TEST_TAG));
      table.delete(d);

      // Since RPC_CODEC_CONF_KEY and DEFAULT_CODEC_CLASS is set to empty, it will use
      // empty Codec and it shouldn't encode/decode tags.
      Scan scan = new Scan().withStartRow(ROW1).setRaw(true);
      ResultScanner scanner = table.getScanner(scan);
      int count = 0;
      Result result;
      while ((result = scanner.next()) != null) {
        List<Cell> cells = result.listCells();
        assertEquals(2, cells.size());
        Cell cell = cells.get(0);
        assertTrue(CellUtil.isDelete(cell));
        List<Tag> tags = PrivateCellUtil.getTags(cell);
        assertEquals(0, tags.size());
        count++;
      }
      assertEquals(1, count);
    } finally {
      UTIL.deleteTable(tableName);
    }
  }
}
