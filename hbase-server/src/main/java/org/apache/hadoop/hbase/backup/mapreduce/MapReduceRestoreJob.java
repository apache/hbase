/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.backup.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.RestoreJob;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.WALPlayer;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.Tool;

/**
 * MapReduce implementation of {@link RestoreJob}
 *
 * For full backup restore, it runs {@link HFileSplitterJob} job and creates
 * HFiles which are aligned with a region boundaries of a table being
 * restored, for incremental backup restore it runs {@link WALPlayer} in
 * bulk load mode (creates HFiles from WAL edits).
 *
 * The resulting HFiles then are loaded using HBase bulk load tool
 * {@link LoadIncrementalHFiles}
 */
@InterfaceAudience.Private
public class MapReduceRestoreJob implements RestoreJob {
  public static final Log LOG = LogFactory.getLog(MapReduceRestoreJob.class);

  private Tool player;
  private Configuration conf;

  public MapReduceRestoreJob() {
  }

  @Override
  public void run(Path[] dirPaths, TableName[] tableNames, TableName[] newTableNames,
      boolean fullBackupRestore) throws IOException {

    String bulkOutputConfKey;

    player = new HFileSplitterJob();
    bulkOutputConfKey = HFileSplitterJob.BULK_OUTPUT_CONF_KEY;
    // Player reads all files in arbitrary directory structure and creates
    // a Map task for each file
    String dirs = StringUtils.join(dirPaths, ",");

    if (LOG.isDebugEnabled()) {
      LOG.debug("Restore " + (fullBackupRestore ? "full" : "incremental")
          + " backup from directory " + dirs + " from hbase tables "
          + StringUtils.join(tableNames, BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND) +
          " to tables "
          + StringUtils.join(newTableNames, BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND));
    }

    for (int i = 0; i < tableNames.length; i++) {

      LOG.info("Restore " + tableNames[i] + " into " + newTableNames[i]);

      Path bulkOutputPath = getBulkOutputDir(getFileNameCompatibleString(newTableNames[i]));
      Configuration conf = getConf();
      conf.set(bulkOutputConfKey, bulkOutputPath.toString());
      String[] playerArgs =
        { dirs,
          fullBackupRestore? newTableNames[i].getNameAsString():tableNames[i].getNameAsString()
        };

      int result = 0;
      int loaderResult = 0;
      try {

        player.setConf(getConf());
        result = player.run(playerArgs);
        if (succeeded(result)) {
          // do bulk load
          LoadIncrementalHFiles loader = createLoader(getConf());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Restoring HFiles from directory " + bulkOutputPath);
          }
          String[] args = { bulkOutputPath.toString(), newTableNames[i].getNameAsString() };
          loaderResult = loader.run(args);

          if (failed(loaderResult)) {
            throw new IOException("Can not restore from backup directory " + dirs
                + " (check Hadoop and HBase logs). Bulk loader return code =" + loaderResult);
          }
        } else {
          throw new IOException("Can not restore from backup directory " + dirs
              + " (check Hadoop/MR and HBase logs). Player return code =" + result);
        }
        LOG.debug("Restore Job finished:" + result);
      } catch (Exception e) {
        throw new IOException("Can not restore from backup directory " + dirs
            + " (check Hadoop and HBase logs) ", e);
      }

    }
  }

  private String getFileNameCompatibleString(TableName table) {
    return table.getNamespaceAsString() + "-" + table.getQualifierAsString();
  }

  private boolean failed(int result) {
    return result != 0;
  }

  private boolean succeeded(int result) {
    return result == 0;
  }

  public static LoadIncrementalHFiles createLoader(Configuration config) throws IOException {
    // set configuration for restore:
    // LoadIncrementalHFile needs more time
    // <name>hbase.rpc.timeout</name> <value>600000</value>
    // calculates
    Integer milliSecInHour = 3600000;
    Configuration conf = new Configuration(config);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, milliSecInHour);

    // By default, it is 32 and loader will fail if # of files in any region exceed this
    // limit. Bad for snapshot restore.
    conf.setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, Integer.MAX_VALUE);
    conf.set(LoadIncrementalHFiles.IGNORE_UNMATCHED_CF_CONF_KEY, "yes");
    LoadIncrementalHFiles loader = null;
    try {
      loader = new LoadIncrementalHFiles(conf);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return loader;
  }

  private Path getBulkOutputDir(String tableName) throws IOException {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    String tmp =
        conf.get(HConstants.TEMPORARY_FS_DIRECTORY_KEY,
          HConstants.DEFAULT_TEMPORARY_HDFS_DIRECTORY);
    Path path =
        new Path(tmp + Path.SEPARATOR + "bulk_output-" + tableName + "-"
            + EnvironmentEdgeManager.currentTime());
    fs.deleteOnExit(path);
    return path;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}
