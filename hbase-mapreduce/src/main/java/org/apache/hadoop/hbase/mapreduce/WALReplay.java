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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALEditInternalHelper;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

/**
 * A tool to replay WAL files as a M/R job. The WAL can be replayed for a set of tables or all
 * tables, and a time range can be provided (in milliseconds). The WAL is filtered to the passed set
 * of tables and the output can optionally be mapped to another set of tables. WAL replay can also
 * replay bulkload operation from WAL files
 */
@InterfaceAudience.Public
public class WALReplay extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(WALReplay.class);
  final static String NAME = "WALReplay";
  public final static String TABLES_KEY = "wal.input.tables";
  public final static String TABLE_MAP_KEY = "wal.input.tablesmap";
  public final static String BULKLOAD_BACKUP_LOCATION = "wal.bulk.backup.location";
  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  public WALReplay() {
  }

  protected WALReplay(final Configuration c) {
    super(c);
  }

  /**
   * Enum for map metrics. Keep it out here rather than inside in the Map inner-class so we can find
   * associated properties.
   */

  protected static enum Counter {
    /** Number of aggregated writes */
    PUTS,
    /** Number of aggregated deletes */
    DELETES,
    CELLS_READ,
    CELLS_WRITTEN,
    WALEDITS
  }

  /**
   * A mapper that writes out {@link Mutation} or list of bulkload files to be directly applied to a
   * running HBase instance.
   */
  protected static class WALMapper
    extends Mapper<WALKey, WALEdit, ImmutableBytesWritable, MutationOrBulkLoad> {
    private Map<TableName, TableName> tables = new TreeMap<>();

    @Override
    public void map(WALKey key, WALEdit value, Context context) throws IOException {
      context.getCounter(Counter.WALEDITS).increment(1);
      try {
        if (tables.isEmpty() || tables.containsKey(key.getTableName())) {
          TableName targetTable =
            tables.isEmpty() ? key.getTableName() : tables.get(key.getTableName());
          ImmutableBytesWritable tableOut = new ImmutableBytesWritable(targetTable.getName());
          Put put = null;
          Delete del = null;
          ExtendedCell lastCell = null;
          for (ExtendedCell cell : WALEditInternalHelper.getExtendedCells(value)) {
            context.getCounter(Counter.CELLS_READ).increment(1);

            // Filtering WAL meta marker entries.
            if (WALEdit.isMetaEditFamily(cell)) {
              continue;
            }

            // Handle BulkLoad WAL entries
            if (CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD)) {
              String namespace = key.getTableName().getNamespaceAsString();
              String tableName = key.getTableName().getQualifierAsString();
              LOG.debug("Processing bulk load for namespace: {}, table: {}", namespace, tableName);

              List<String> bulkloadFiles = handleBulkLoadCell(cell);
              LOG.debug("Found {} bulk load files for table: {}", bulkloadFiles.size(), tableName);

              // Prefix each file path with namespace and table name to construct the full paths
              List<String> bulkloadFilesWithFullPath = new ArrayList<>();
              for (String filePath : bulkloadFiles) {
                String fullPath = new Path(namespace, new Path(tableName, filePath)).toString();
                bulkloadFilesWithFullPath.add(fullPath);
              }
              LOG.debug("Bulk load files with full paths: {}", bulkloadFilesWithFullPath.size());

              // Retrieve configuration and set up file systems for backup and staging locations
              Configuration conf = context.getConfiguration();
              Path backupLocation = new Path(conf.get(BULKLOAD_BACKUP_LOCATION));
              FileSystem rootFs = CommonFSUtils.getRootDirFileSystem(conf); // HDFS filesystem
              Path hbaseStagingDir =
                new Path(CommonFSUtils.getRootDir(conf), HConstants.BULKLOAD_STAGING_DIR_NAME);
              FileSystem backupFs = FileSystem.get(backupLocation.toUri(), conf);
              List<String> stagingPaths = new ArrayList<>();

              try {
                for (String file : bulkloadFilesWithFullPath) {
                  // Full file path from backupLocation
                  Path fullBackupFilePath = new Path(backupLocation, file);
                  // Staging path on HDFS
                  Path stagingPath = new Path(hbaseStagingDir, file);

                  LOG.info("Copying file from backup location: {} to HDFS staging: {}",
                    fullBackupFilePath, stagingPath);
                  // Copy the file from backupLocation to HDFS
                  FileUtil.copy(backupFs, fullBackupFilePath, rootFs, stagingPath, false, conf);

                  stagingPaths.add(stagingPath.toString());
                }
              } catch (IOException e) {
                LOG.error("Error copying files for bulk load: {}", e.getMessage(), e);
                throw new IOException("Failed to copy files for bulk load.", e);
              }
              context.write(tableOut, MutationOrBulkLoad.fromBulkLoadFiles(stagingPaths));
            }

            // Allow a subclass filter out this cell.
            if (filter(context, cell)) {
              // A WALEdit may contain multiple operations (HBASE-3584) and/or
              // multiple rows (HBASE-5229).
              // Aggregate as much as possible into a single Put/Delete
              // operation before writing to the context.
              if (
                lastCell == null || lastCell.getTypeByte() != cell.getTypeByte()
                  || !CellUtil.matchingRows(lastCell, cell)
              ) {
                // row or type changed, write out aggregate KVs.
                if (put != null) {
                  context.write(tableOut, MutationOrBulkLoad.fromMutation(put));
                  context.getCounter(Counter.PUTS).increment(1);
                }
                if (del != null) {
                  context.write(tableOut, MutationOrBulkLoad.fromMutation(del));
                  context.getCounter(Counter.DELETES).increment(1);
                }
                if (CellUtil.isDelete(cell)) {
                  del = new Delete(CellUtil.cloneRow(cell));
                } else {
                  put = new Put(CellUtil.cloneRow(cell));
                }
              }
              if (CellUtil.isDelete(cell)) {
                del.add(cell);
              } else {
                put.add(cell);
              }
              context.getCounter(Counter.CELLS_WRITTEN).increment(1);
            }
            lastCell = cell;
          }
          // write residual KVs
          if (put != null) {
            context.write(tableOut, MutationOrBulkLoad.fromMutation(put));
            context.getCounter(Counter.PUTS).increment(1);
          }
          if (del != null) {
            context.write(tableOut, MutationOrBulkLoad.fromMutation(del));
            context.getCounter(Counter.DELETES).increment(1);
          }
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted while writing results", e);
        Thread.currentThread().interrupt();
      }
    }

    protected boolean filter(Context context, final Cell cell) {
      return true;
    }

    private List<String> handleBulkLoadCell(Cell cell) throws IOException {
      List<String> resultFiles = new ArrayList<>();
      LOG.debug("Bulk load detected in cell. Processing...");

      WALProtos.BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cell);

      if (bld == null) {
        LOG.warn("BulkLoadDescriptor is null for cell: {}", cell);
        return resultFiles;
      }
      if (!bld.getReplicate()) {
        LOG.warn("Replication is disabled for bulk load cell: {}", cell);
      }

      String regionName = bld.getEncodedRegionName().toStringUtf8();
      List<WALProtos.StoreDescriptor> storesList = bld.getStoresList();
      if (storesList == null) {
        LOG.warn("Store descriptor list is null for region: {}", regionName);
        return resultFiles;
      }

      for (WALProtos.StoreDescriptor storeDescriptor : storesList) {
        String columnFamilyName = storeDescriptor.getFamilyName().toStringUtf8();
        LOG.debug("Processing column family: {}", columnFamilyName);

        List<String> storeFileList = storeDescriptor.getStoreFileList();
        if (storeFileList == null) {
          LOG.warn("Store file list is null for column family: {}", columnFamilyName);
          continue;
        }

        for (String storeFile : storeFileList) {
          String hFilePath = getHFilePath(regionName, columnFamilyName, storeFile);
          LOG.debug("Adding HFile path to bulk load file paths: {}", hFilePath);
          resultFiles.add(hFilePath);
        }
      }
      return resultFiles;
    }

    private String getHFilePath(String regionName, String columnFamilyName, String storeFileName) {
      return new Path(regionName, new Path(columnFamilyName, storeFileName)).toString();
    }

    @Override
    protected void
      cleanup(Mapper<WALKey, WALEdit, ImmutableBytesWritable, MutationOrBulkLoad>.Context context)
        throws IOException, InterruptedException {
      super.cleanup(context);
    }

    @SuppressWarnings("checkstyle:EmptyBlock")
    @Override
    public void setup(Context context) throws IOException {
      String[] tableMap = context.getConfiguration().getStrings(TABLE_MAP_KEY);
      String[] tablesToUse = context.getConfiguration().getStrings(TABLES_KEY);
      if (tableMap == null) {
        tableMap = tablesToUse;
      }
      if (tablesToUse == null) {
        // Then user wants all tables.
      } else if (tablesToUse.length != tableMap.length) {
        // this can only happen when WALMapper is used directly by a class other than WALReplay
        throw new IOException("Incorrect table mapping specified .");
      }
      int i = 0;
      if (tablesToUse != null) {
        for (String table : tablesToUse) {
          tables.put(TableName.valueOf(table), TableName.valueOf(tableMap[i++]));
        }
      }
    }
  }

  void setupTime(Configuration conf, String option) throws IOException {
    String val = conf.get(option);
    if (null == val) {
      return;
    }
    long ms;
    try {
      // first try to parse in user friendly form
      ms = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS").parse(val).getTime();
    } catch (ParseException pe) {
      try {
        // then see if just a number of ms's was specified
        ms = Long.parseLong(val);
      } catch (NumberFormatException nfe) {
        throw new IOException(
          option + " must be specified either in the form 2001-02-20T16:35:06.99 "
            + "or as number of milliseconds");
      }
    }
    conf.setLong(option, ms);
  }

  /**
   * Sets up the actual job.
   * @param args The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public Job createSubmittableJob(String[] args) throws IOException {
    Configuration conf = getConf();
    setupTime(conf, WALInputFormat.START_TIME_KEY);
    setupTime(conf, WALInputFormat.END_TIME_KEY);
    String inputDirs = args[0];
    String walDir = new Path(inputDirs, "WALs").toString();
    String bulkLoadFilesDir = new Path(inputDirs, "bulk-load-files").toString();
    String[] tables = args.length == 1 ? new String[] {} : args[1].split(",");
    String[] tableMap;
    if (args.length > 2) {
      tableMap = args[2].split(",");
      if (tableMap.length != tables.length) {
        throw new IOException("The same number of tables and mapping must be provided.");
      }
    } else {
      // if no mapping is specified, map each table to itself
      tableMap = tables;
    }
    conf.setStrings(TABLES_KEY, tables);
    conf.setStrings(TABLE_MAP_KEY, tableMap);
    conf.set(FileInputFormat.INPUT_DIR, walDir);
    conf.set(BULKLOAD_BACKUP_LOCATION, bulkLoadFilesDir);
    Job job = Job.getInstance(conf,
      conf.get(JOB_NAME_CONF_KEY, NAME + "_" + EnvironmentEdgeManager.currentTime()));
    job.setJarByClass(WALReplay.class);
    job.setInputFormatClass(WALInputFormat.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapperClass(WALMapper.class);
    job.setOutputFormatClass(MultiTableOutputFormatWalReplay.class);
    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.initCredentials(job);
    // No reducers.
    job.setNumReduceTasks(0);
    String codecCls = WALCellCodec.getWALCellCodecClass(conf).getName();
    try {
      TableMapReduceUtil.addDependencyJarsForClasses(job.getConfiguration(),
        Class.forName(codecCls));
    } catch (Exception e) {
      throw new IOException("Cannot determine wal codec class " + codecCls, e);
    }
    return job;
  }

  private List<TableName> getTableNameList(String[] tables) {
    List<TableName> list = new ArrayList<TableName>();
    for (String name : tables) {
      list.add(TableName.valueOf(name));
    }
    return list;
  }

  /**
   * Print usage
   * @param errorMsg Error message. Can be null.
   */
  private void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: " + NAME + " [options] <WAL inputdir> [<tables> <tableMappings>]");
    System.err.println(" <WAL inputdir>   directory of WALs to replay.");
    System.err.println(" <tables>         comma separated list of tables. If no tables specified,");
    System.err.println("                  all are imported (even hbase:meta if present).");
    System.err.println(
      " <tableMappings>  WAL entries can be mapped to a new set of tables by " + "passing");
    System.err
      .println("                  <tableMappings>, a comma separated list of target " + "tables.");
    System.err
      .println("                  If specified, each table in <tables> must have a " + "mapping.");
    System.err.println("To specify a time range, pass:");
    System.err.println(" -D" + WALInputFormat.START_TIME_KEY + "=[date|ms]");
    System.err.println(" -D" + WALInputFormat.END_TIME_KEY + "=[date|ms]");
    System.err.println(" The start and the end date of timerange (inclusive). The dates can be");
    System.err
      .println(" expressed in milliseconds-since-epoch or yyyy-MM-dd'T'HH:mm:ss.SS " + "format.");
    System.err.println(" E.g. 1234567890120 or 2009-02-13T23:32:30.12");
    System.err.println("Other options:");
    System.err.println(" -D" + JOB_NAME_CONF_KEY + "=jobName");
    System.err.println(" Use the specified mapreduce job name for the wal player");
    System.err.println(" -Dwal.input.separator=' '");
    System.err.println(" Change WAL filename separator (WAL dir names use default ','.)");
    System.err.println("For performance also consider the following options:\n"
      + "  -Dmapreduce.map.speculative=false\n" + "  -Dmapreduce.reduce.speculative=false");
  }

  /**
   * Main entry point.
   * @param args The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new WALReplay(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      usage("Wrong number of arguments: " + args.length);
      System.exit(-1);
    }
    Job job = createSubmittableJob(args);
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
