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
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Make a copy of entire table using bulkupload</li>
 */
public class TableCopyUtils {

  private Configuration conf;
  private HBaseAdmin admin;
  private long startTime;
  private long endTime;
  private String newTableName;
  private String oldTableName;


  public TableCopyUtils() throws MasterNotRunningException {
    this.conf = HBaseConfiguration.create();
    this.admin = new HBaseAdmin(conf);
    this.startTime = 0;
    this.endTime = 0;
  }


  /**
   * Makes exact copy of another table with the specified name using bulkload
   * @param args - arguments with which we run the tool
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  public void copyTableWithBulkUpload(String[] args) throws IOException,
      ClassNotFoundException, InterruptedException {
    if (!doCommandLine(args)) {
      return;
    }
    HTable oldTable = new HTable(conf, oldTableName);
    // temp files will be stored here when data is loaded with the MR job
    Path outputPath = new Path("COPYTABLE-" + oldTableName + "-" + newTableName);
    HColumnDescriptor[] families = oldTable.getTableDescriptor()
        .getColumnFamilies();
    Scan scan = new Scan();
    if (this.startTime != 0) {
      scan.setTimeRange(startTime,
          this.endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
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

  private static void printUsage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: TableCopyUtils " +
        "[--starttime=X] [--endtime=Y] " +
        "<newtablename> <tablename>");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" starttime    beginning of the time range");
    System.err.println("              without endtime means from starttime to forever");
    System.err.println(" endtime      end of the time range");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" newname      Name of the new table");
    System.err.println(" tablename    Name of the table to copy");
    System.err.println();
    System.err.println("Example:");
    System.err.println(" $ bin/hbase " +
        "org.apache.hadoop.hbase.mapreduce.TableCopyUtils --starttime=1265875194289 --endtime=1265878794289 " +
        "NewTable OriginalTable ");
  }

  private boolean doCommandLine(final String[] args) {
    if (args.length < 1) {
      printUsage(null);
      return false;
    }
    try {
      for (int i = 0; i < args.length; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage(null);
          return false;
        }
        final String startTimeArgKey = "--starttime=";
        if (cmd.startsWith(startTimeArgKey)) {
          this.startTime = Long.parseLong(cmd.substring(startTimeArgKey.length()));
          continue;
        }
        final String endTimeArgKey = "--endtime=";
        if (cmd.startsWith(endTimeArgKey)) {
          this.endTime = Long.parseLong(cmd.substring(endTimeArgKey.length()));
          continue;
        }
        if (i == args.length - 1) {
          this.oldTableName = cmd;
        }
        if (i == args.length - 2) {
          this.newTableName = cmd;
        }
      }
      if (this.newTableName == null) {
        printUsage("New table name must be specified");
        return false;
      }
      if (this.oldTableName == null) {
        printUsage("Original table must be specified");
        return false;
      }
    } catch (Exception e) {
      e.printStackTrace();
      printUsage("Can't start because " + e.getMessage());
      return false;
    }
    return true;
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

  public static void main(String[] args) throws IOException, ParseException,
      ClassNotFoundException, InterruptedException {
    TableCopyUtils tcu = new TableCopyUtils();
    tcu.copyTableWithBulkUpload(args);
  }
}
