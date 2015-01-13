/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test performance improvement of joined scanners optimization:
 * https://issues.apache.org/jira/browse/HBASE-5416
 */
@Category({RegionServerTests.class, LargeTests.class})
public class TestJoinedScanners {
  static final Log LOG = LogFactory.getLog(TestJoinedScanners.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String DIR = TEST_UTIL.getDataTestDir("TestJoinedScanners").toString();

  private static final byte[] cf_essential = Bytes.toBytes("essential");
  private static final byte[] cf_joined = Bytes.toBytes("joined");
  private static final byte[] col_name = Bytes.toBytes("a");
  private static final byte[] flag_yes = Bytes.toBytes("Y");
  private static final byte[] flag_no  = Bytes.toBytes("N");

  private static DataBlockEncoding blockEncoding = DataBlockEncoding.FAST_DIFF;
  private static int selectionRatio = 30;
  private static int valueWidth = 128 * 1024;

  @Test
  public void testJoinedScanners() throws Exception {
    String dataNodeHosts[] = new String[] { "host1", "host2", "host3" };
    int regionServersCount = 3;

    HBaseTestingUtility htu = new HBaseTestingUtility();

    final int DEFAULT_BLOCK_SIZE = 1024*1024;
    htu.getConfiguration().setLong("dfs.blocksize", DEFAULT_BLOCK_SIZE);
    htu.getConfiguration().setInt("dfs.replication", 1);
    htu.getConfiguration().setLong("hbase.hregion.max.filesize", 322122547200L);
    MiniHBaseCluster cluster = null;

    try {
      cluster = htu.startMiniCluster(1, regionServersCount, dataNodeHosts);
      byte [][] families = {cf_essential, cf_joined};

      TableName tableName = TableName.valueOf(this.getClass().getSimpleName());
      HTableDescriptor desc = new HTableDescriptor(tableName);
      for(byte[] family : families) {
        HColumnDescriptor hcd = new HColumnDescriptor(family);
        hcd.setDataBlockEncoding(blockEncoding);
        desc.addFamily(hcd);
      }
      htu.getHBaseAdmin().createTable(desc);
      Table ht = htu.getConnection().getTable(tableName);

      long rows_to_insert = 1000;
      int insert_batch = 20;
      long time = System.nanoTime();
      Random rand = new Random(time);

      LOG.info("Make " + Long.toString(rows_to_insert) + " rows, total size = "
        + Float.toString(rows_to_insert * valueWidth / 1024 / 1024) + " MB");

      byte [] val_large = new byte[valueWidth];

      List<Put> puts = new ArrayList<Put>();

      for (long i = 0; i < rows_to_insert; i++) {
        Put put = new Put(Bytes.toBytes(Long.toString (i)));
        if (rand.nextInt(100) <= selectionRatio) {
          put.add(cf_essential, col_name, flag_yes);
        } else {
          put.add(cf_essential, col_name, flag_no);
        }
        put.add(cf_joined, col_name, val_large);
        puts.add(put);
        if (puts.size() >= insert_batch) {
          ht.put(puts);
          puts.clear();
        }
      }
      if (puts.size() >= 0) {
        ht.put(puts);
        puts.clear();
      }

      LOG.info("Data generated in "
        + Double.toString((System.nanoTime() - time) / 1000000000.0) + " seconds");

      boolean slow = true;
      for (int i = 0; i < 10; ++i) {
        runScanner(ht, slow);
        slow = !slow;
      }

      ht.close();
    } finally {
      if (cluster != null) {
        htu.shutdownMiniCluster();
      }
    }
  }

  private void runScanner(Table table, boolean slow) throws Exception {
    long time = System.nanoTime();
    Scan scan = new Scan();
    scan.addColumn(cf_essential, col_name);
    scan.addColumn(cf_joined, col_name);

    SingleColumnValueFilter filter = new SingleColumnValueFilter(
        cf_essential, col_name, CompareFilter.CompareOp.EQUAL, flag_yes);
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    scan.setLoadColumnFamiliesOnDemand(!slow);

    ResultScanner result_scanner = table.getScanner(scan);
    Result res;
    long rows_count = 0;
    while ((res = result_scanner.next()) != null) {
      rows_count++;
    }

    double timeSec = (System.nanoTime() - time) / 1000000000.0;
    result_scanner.close();
    LOG.info((slow ? "Slow" : "Joined") + " scanner finished in " + Double.toString(timeSec)
      + " seconds, got " + Long.toString(rows_count/2) + " rows");
  }

  private static Options options = new Options();

  /**
   * Command line interface:
   * @param args
   * @throws IOException if there is a bug while reading from disk
   */
  public static void main(final String[] args) throws Exception {
    Option encodingOption = new Option("e", "blockEncoding", true,
      "Data block encoding; Default: FAST_DIFF");
    encodingOption.setRequired(false);
    options.addOption(encodingOption);
    
    Option ratioOption = new Option("r", "selectionRatio", true,
      "Ratio of selected rows using essential column family");
    ratioOption.setRequired(false);
    options.addOption(ratioOption);
    
    Option widthOption = new Option("w", "valueWidth", true,
      "Width of value for non-essential column family");
    widthOption.setRequired(false);
    options.addOption(widthOption);
    
    CommandLineParser parser = new GnuParser();
    CommandLine cmd = parser.parse(options, args);
    if (args.length < 1) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("TestJoinedScanners", options, true);
    }
    
    if (cmd.hasOption("e")) {
      blockEncoding = DataBlockEncoding.valueOf(cmd.getOptionValue("e"));
    }
    if (cmd.hasOption("r")) {
      selectionRatio = Integer.parseInt(cmd.getOptionValue("r"));
    }
    if (cmd.hasOption("w")) {
      valueWidth = Integer.parseInt(cmd.getOptionValue("w"));
    }
    // run the test
    TestJoinedScanners test = new TestJoinedScanners();
    test.testJoinedScanners();
  }
}
