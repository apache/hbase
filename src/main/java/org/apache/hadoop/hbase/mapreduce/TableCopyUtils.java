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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Util which can do two things:
 * <ul>
 *  <li>Make a copy of entire table using bulkupload</li>
 *  <li>Make a smaller table out of a bigger one</li>
 * </ul>
 */
public class TableCopyUtils {

  private Configuration conf;
  private HBaseAdmin admin;

  public TableCopyUtils() throws MasterNotRunningException {
    this.conf = HBaseConfiguration.create();
    this.admin = new HBaseAdmin(conf);
  }

  /**
   * Creates a table out of limited number of KVs and regions from an old table
   *
   * @param numRegions
   * @param numKVs
   * @param oldTable
   * @param newTableName
   * @throws IOException
   */
  public void createTableFromRegions(int numRegions, int numKVs,
      String oldTable, String newTableName) throws IOException {
    HTable tableOld = new HTable(conf, oldTable);
    HColumnDescriptor[] families = tableOld.getTableDescriptor()
        .getColumnFamilies();
    createTable(newTableName, families, tableOld.getStartKeys());
    HTable tableNew = new HTable(conf, newTableName);
    Set<HRegionInfo> allRegions = tableOld.getRegionsInfo().keySet();
    List<Put> puts = new ArrayList<Put>();
    int i = 0;
    int currentKvs = 0;
    for (HRegionInfo info : allRegions) {
      if (i == numRegions) {
        break;
      }
      ResultScanner scanner = tableOld.getScanner(new Scan(info.getStartKey(),
          info.getEndKey()));
      for (Iterator<Result> iterator = scanner.iterator(); iterator.hasNext();) {
        Result result = (Result) iterator.next();
        List<KeyValue> kvs = result.list();
        for (KeyValue kv : kvs) {
          Put p = new Put(kv.getRow());
          p.add(kv);
          puts.add(p);
          currentKvs++;
          if (numKVs != -1 && currentKvs == numKVs) {
            tableNew.put(puts);
            return;
          }
        }
      }
      scanner.close();
      i++;
    }
    tableNew.put(puts);
  }

  /**
   * Makes exact copy of another table with the specified name using bulkload
   *
   * @param oldTableName the table that we want to copy
   * @param startRow optional parameter if we want to scan & copy smaller portion of the table
   * @param stopRow optional parameter if we want to scan & copy smaller portion of the table
   * @param newTableName name of the new table that will be created
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  public void copyTableWithBulkUpload(String oldTableName, byte[] startRow, byte[] stopRow,
      String newTableName) throws IOException, ClassNotFoundException,
      InterruptedException {
    HTable oldTable = new HTable(conf, oldTableName);
    // temp files will be stored here when data is loaded with the MR job
    Path outputPath = new Path("OUTPUT-" + oldTableName + "-" + newTableName);
    HColumnDescriptor[] families = oldTable.getTableDescriptor()
        .getColumnFamilies();
    Scan scan = new Scan();
    if (startRow.length != 0) {
      scan.setStartRow(startRow);
      if (stopRow.length != 0) {
        scan.setStopRow(stopRow);
      }
    }
    for (HColumnDescriptor descriptor : families) {
      scan.addFamily(descriptor.getName());
    }
    // Create and initialize the MR job
    Job job = new Job(conf, "process column contents");
    FileOutputFormat.setOutputPath(job, outputPath);

    TableMapReduceUtil.initHTableInputAndHFileOutputMapperJob(oldTableName,
        scan, KeyValueMapper.class, job);
    // Wait for job completion
    job.waitForCompletion(true);
    byte[][] startKeys = oldTable.getStartKeys();
    createTable(newTableName, families, startKeys);
    HTable newTable = new HTable(conf, newTableName);

    // Bulk upload the MR output file into HBase
    new LoadIncrementalHFiles(conf).doBulkLoad(outputPath, newTable);
    // delete the bulk-uploaded file
    final FileSystem fs = outputPath.getFileSystem(conf);
    fs.delete(outputPath, true);
  }

  private void createTable(String newTableName, HColumnDescriptor[] families, byte[][] startKeys)
      throws IOException {
    HTableDescriptor desc = new HTableDescriptor(newTableName);
    for (HColumnDescriptor colDesc : families) {
      desc.addFamily(colDesc);
    }
    admin.createTable(desc, startKeys);
  }

  public static void main(String[] args) throws IOException, ParseException,
      ClassNotFoundException, InterruptedException {
    Options opt = new Options();
    opt.addOption("bl", "bulkupload", false,
        "copy table with bulk upload method");
    opt.addOption("lc", "limited copy", false,
        "copy just a number of keys and regions from the old table to a new table");
    CommandLine cmd = new GnuParser().parse(opt, args);
    TableCopyUtils tcu = new TableCopyUtils();
    Scanner s = new Scanner(System.in);
    System.out.print("Name of the old table: ");
    String oldTableName = s.next();
    System.out.print("Name of the new table: ");
    String newTableName = s.next();
    if (cmd.hasOption("bl")) {
      byte[] startRow = new byte[] {};
      byte[] stopRow = new byte[] {};
      System.out.print("Do you want to set start and stop row for the old table scanner? [y/n] ");
      if (s.next().trim().toLowerCase().equals("y")) {
        System.out
            .print("Insert b (for setting start and stop row) OR s (for setting start row)");
        String input = s.next().trim().toLowerCase();
        if (input.equals("b") || input.equals("s")) {
          System.out.print("Start row: ");
          startRow = Bytes.toBytes(s.nextLine());
          if (input.equals("b")) {
            System.out.print("Stop row: ");
            stopRow = Bytes.toBytes(s.nextLine());
          }
        }
      }
      tcu.copyTableWithBulkUpload(oldTableName, startRow, stopRow, newTableName);
    } else if (cmd.hasOption("lc")) {
      System.out
          .println("Use this method only if you are copying a very small portion of the table..");
      System.out.print("Number of regions to be copied: ");
      int numRegions = s.nextInt();
      System.out
          .println("Number of KVs to be copied: (if you don't want to limit the number of KVs insert -1)");
      int numKVs = s.nextInt();
      tcu.createTableFromRegions(numRegions, numKVs, oldTableName, newTableName);
    }
    s.close();
  }

  public static class KeyValueMapper extends
      TableMapper<ImmutableBytesWritable, KeyValue> {
    public void map(ImmutableBytesWritable key, Result result, Context context)
        throws IOException, InterruptedException {
      List<KeyValue> kvs = result.list();
      for (KeyValue kv : kvs) {
        context.write(key, kv);
      }
    }
  }
}
