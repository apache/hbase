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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.util.Iterator;
import java.util.Map;

@Category(MediumTests.class)
public class TestImportExport {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final String FAMILYA_STRING = "a";
  private static final String FAMILYB_STRING = "b";
  private static final byte[] FAMILYA = Bytes.toBytes(FAMILYA_STRING);
  private static final byte[] FAMILYB = Bytes.toBytes(FAMILYB_STRING);
  private static final byte[] QUAL = Bytes.toBytes("q");
  private static final String OUTPUT_DIR = "outputdir";

  private static MiniHBaseCluster cluster;
  private static long now = System.currentTimeMillis();

  @BeforeClass
  public static void beforeClass() throws Exception {
    cluster = UTIL.startMiniCluster();
    UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniMapReduceCluster();
    UTIL.shutdownMiniCluster();
  }

  @Before
  @After
  public void cleanup() throws Exception {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    fs.delete(new Path(OUTPUT_DIR), true);
  }

  /**
   * When running on Hadoop 2, we need to copy (or add) configuration values for keys
   * that start with "yarn." (from the map reduce minicluster) to the
   * configuration that will be used during the test (from the HBase minicluster). 
   * YARN configuration values are set properly in the map reduce minicluster, 
   * but not necessarily in the HBase mini cluster.
   * @param srcConf the configuration to copy from (the map reduce minicluster version)
   * @param destConf the configuration to copy to (the HBase minicluster version)
   */
  private void copyConfigurationValues(Configuration srcConf, Configuration destConf) {
    Iterator<Map.Entry<String,String>> it = srcConf.iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      String key = entry.getKey();
      String value = entry.getValue();
      if (key.startsWith("yarn.") && !value.isEmpty()) {
        destConf.set(key, value);
      }
    }
  }

  /**
   * Test simple replication case with column mapping
   * @throws Exception
   */
  @Test
  public void testSimpleCase() throws Exception {
    String EXPORT_TABLE = "exportSimpleCase";
    HTable t = UTIL.createTable(Bytes.toBytes(EXPORT_TABLE), FAMILYA);
    Put p = new Put(ROW1);
    p.add(FAMILYA, QUAL, now, QUAL);
    p.add(FAMILYA, QUAL, now+1, QUAL);
    p.add(FAMILYA, QUAL, now+2, QUAL);
    t.put(p);
    p = new Put(ROW2);
    p.add(FAMILYA, QUAL, now, QUAL);
    p.add(FAMILYA, QUAL, now+1, QUAL);
    p.add(FAMILYA, QUAL, now+2, QUAL);
    t.put(p);

    String[] args = new String[] {
        EXPORT_TABLE,
        OUTPUT_DIR,
        "1000"
    };

    GenericOptionsParser opts = new GenericOptionsParser(new Configuration(cluster.getConfiguration()), args);
    Configuration conf = opts.getConfiguration();

    // copy or add the necessary configuration values from the map reduce config to the hbase config
    copyConfigurationValues(UTIL.getConfiguration(), conf);
    args = opts.getRemainingArgs();

    Job job = Export.createSubmittableJob(conf, args);
    job.getConfiguration().set("mapreduce.framework.name", "yarn");
    job.waitForCompletion(false);
    assertTrue(job.isSuccessful());


    String IMPORT_TABLE = "importTableSimpleCase";
    t = UTIL.createTable(Bytes.toBytes(IMPORT_TABLE), FAMILYB);
    args = new String[] {
        "-D" + Import.CF_RENAME_PROP + "="+FAMILYA_STRING+":"+FAMILYB_STRING,
        IMPORT_TABLE,
        OUTPUT_DIR
    };

    opts = new GenericOptionsParser(new Configuration(cluster.getConfiguration()), args);
    conf = opts.getConfiguration();

    // copy or add the necessary configuration values from the map reduce config to the hbase config
    copyConfigurationValues(UTIL.getConfiguration(), conf);
    args = opts.getRemainingArgs();

    job = Import.createSubmittableJob(conf, args);
    job.getConfiguration().set("mapreduce.framework.name", "yarn");
    job.waitForCompletion(false);
    assertTrue(job.isSuccessful());

    Get g = new Get(ROW1);
    g.setMaxVersions();
    Result r = t.get(g);
    assertEquals(3, r.size());
    g = new Get(ROW2);
    g.setMaxVersions();
    r = t.get(g);
    assertEquals(3, r.size());
  }

  /**
   * Test export .META. table
   * 
   * @throws Exception
   */
  @Test
  public void testMetaExport() throws Exception {
    String EXPORT_TABLE = ".META.";
    String[] args = new String[] { EXPORT_TABLE, OUTPUT_DIR, "1", "0", "0" };
    GenericOptionsParser opts = new GenericOptionsParser(new Configuration(
        cluster.getConfiguration()), args);
    Configuration conf = opts.getConfiguration();

    // copy or add the necessary configuration values from the map reduce config to the hbase config
    copyConfigurationValues(UTIL.getConfiguration(), conf);
    args = opts.getRemainingArgs();

    Job job = Export.createSubmittableJob(conf, args);
    job.getConfiguration().set("mapreduce.framework.name", "yarn");
    job.waitForCompletion(false);
    assertTrue(job.isSuccessful());
  }

  @Test
  public void testWithDeletes() throws Exception {
    String EXPORT_TABLE = "exportWithDeletes";
    HTableDescriptor desc = new HTableDescriptor(EXPORT_TABLE);
    desc.addFamily(new HColumnDescriptor(FAMILYA)
        .setMaxVersions(5)
        .setKeepDeletedCells(true)
    );
    UTIL.getHBaseAdmin().createTable(desc);
    HTable t = new HTable(UTIL.getConfiguration(), EXPORT_TABLE);

    Put p = new Put(ROW1);
    p.add(FAMILYA, QUAL, now, QUAL);
    p.add(FAMILYA, QUAL, now+1, QUAL);
    p.add(FAMILYA, QUAL, now+2, QUAL);
    p.add(FAMILYA, QUAL, now+3, QUAL);
    p.add(FAMILYA, QUAL, now+4, QUAL);
    t.put(p);

    Delete d = new Delete(ROW1, now+3, null);
    t.delete(d);
    d = new Delete(ROW1);
    d.deleteColumns(FAMILYA, QUAL, now+2);
    t.delete(d);
    
    String[] args = new String[] {
        "-D" + Export.RAW_SCAN + "=true",
        EXPORT_TABLE,
        OUTPUT_DIR,
        "1000"
    };

    GenericOptionsParser opts = new GenericOptionsParser(new Configuration(cluster.getConfiguration()), args);
    Configuration conf = opts.getConfiguration();

    // copy or add the necessary configuration values from the map reduce config to the hbase config
    copyConfigurationValues(UTIL.getConfiguration(), conf);
    args = opts.getRemainingArgs();

    Job job = Export.createSubmittableJob(conf, args);
    job.getConfiguration().set("mapreduce.framework.name", "yarn");
    job.waitForCompletion(false);
    assertTrue(job.isSuccessful());


    String IMPORT_TABLE = "importWithDeletes";
    desc = new HTableDescriptor(IMPORT_TABLE);
    desc.addFamily(new HColumnDescriptor(FAMILYA)
        .setMaxVersions(5)
        .setKeepDeletedCells(true)
    );
    UTIL.getHBaseAdmin().createTable(desc);
    t.close();
    t = new HTable(UTIL.getConfiguration(), IMPORT_TABLE);
    args = new String[] {
        IMPORT_TABLE,
        OUTPUT_DIR
    };

    opts = new GenericOptionsParser(new Configuration(cluster.getConfiguration()), args);
    conf = opts.getConfiguration();

    // copy or add the necessary configuration values from the map reduce config to the hbase config
    copyConfigurationValues(UTIL.getConfiguration(), conf);
    args = opts.getRemainingArgs();

    job = Import.createSubmittableJob(conf, args);
    job.getConfiguration().set("mapreduce.framework.name", "yarn");
    job.waitForCompletion(false);
    assertTrue(job.isSuccessful());

    Scan s = new Scan();
    s.setMaxVersions();
    s.setRaw(true);
    ResultScanner scanner = t.getScanner(s);
    Result r = scanner.next();
    KeyValue[] res = r.raw();
    assertTrue(res[0].isDeleteFamily());
    assertEquals(now+4, res[1].getTimestamp());
    assertEquals(now+3, res[2].getTimestamp());
    assertTrue(res[3].isDelete());
    assertEquals(now+2, res[4].getTimestamp());
    assertEquals(now+1, res[5].getTimestamp());
    assertEquals(now, res[6].getTimestamp());
    t.close();
  }

  @Test
  public void testWithFilter() throws Exception {
    String EXPORT_TABLE = "exportSimpleCase_ImportWithFilter";
    HTableDescriptor desc = new HTableDescriptor(EXPORT_TABLE);
    desc.addFamily(new HColumnDescriptor(FAMILYA).setMaxVersions(5));
    UTIL.getHBaseAdmin().createTable(desc);
    HTable exportTable = new HTable(UTIL.getConfiguration(), EXPORT_TABLE);

    Put p = new Put(ROW1);
    p.add(FAMILYA, QUAL, now, QUAL);
    p.add(FAMILYA, QUAL, now + 1, QUAL);
    p.add(FAMILYA, QUAL, now + 2, QUAL);
    p.add(FAMILYA, QUAL, now + 3, QUAL);
    p.add(FAMILYA, QUAL, now + 4, QUAL);
    exportTable.put(p);

    String[] args = new String[] { EXPORT_TABLE, OUTPUT_DIR, "1000" };

    GenericOptionsParser opts = new GenericOptionsParser(new Configuration(
        cluster.getConfiguration()), args);
    Configuration conf = opts.getConfiguration();
    args = opts.getRemainingArgs();

    Job job = Export.createSubmittableJob(conf, args);
    job.getConfiguration().set("mapreduce.framework.name", "yarn");
    job.waitForCompletion(false);
    assertTrue(job.isSuccessful());

    String IMPORT_TABLE = "importWithFilter";
    desc = new HTableDescriptor(IMPORT_TABLE);
    desc.addFamily(new HColumnDescriptor(FAMILYA).setMaxVersions(5));
    UTIL.getHBaseAdmin().createTable(desc);

    HTable importTable = new HTable(UTIL.getConfiguration(), IMPORT_TABLE);
    args = new String[] { "-D" + Import.FILTER_CLASS_CONF_KEY + "=" + PrefixFilter.class.getName(),
        "-D" + Import.FILTER_ARGS_CONF_KEY + "=" + Bytes.toString(ROW1), IMPORT_TABLE, OUTPUT_DIR,
        "1000" };

    opts = new GenericOptionsParser(new Configuration(cluster.getConfiguration()), args);
    conf = opts.getConfiguration();
    args = opts.getRemainingArgs();

    job = Import.createSubmittableJob(conf, args);
    job.getConfiguration().set("mapreduce.framework.name", "yarn");
    job.waitForCompletion(false);
    assertTrue(job.isSuccessful());

    // get the count of the source table for that time range
    PrefixFilter filter = new PrefixFilter(ROW1);
    int count = getCount(exportTable, filter);

    Assert.assertEquals("Unexpected row count between export and import tables", count,
      getCount(importTable, null));

    // and then test that a broken command doesn't bork everything - easier here because we don't
    // need to re-run the export job

    args = new String[] { "-D" + Import.FILTER_CLASS_CONF_KEY + "=" + Filter.class.getName(),
        "-D" + Import.FILTER_ARGS_CONF_KEY + "=" + Bytes.toString(ROW1) + "", EXPORT_TABLE,
        OUTPUT_DIR, "1000" };

    opts = new GenericOptionsParser(new Configuration(cluster.getConfiguration()), args);
    conf = opts.getConfiguration();
    args = opts.getRemainingArgs();

    job = Import.createSubmittableJob(conf, args);
    job.getConfiguration().set("mapreduce.framework.name", "yarn");
    job.waitForCompletion(false);
    assertFalse("Job succeeedd, but it had a non-instantiable filter!", job.isSuccessful());

    // cleanup
    exportTable.close();
    importTable.close();
  }

  /**
   * Count the number of keyvalues in the specified table for the given timerange
   * @param start
   * @param end
   * @param table
   * @return
   * @throws IOException
   */
  private int getCount(HTable table, Filter filter) throws IOException {
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
}
