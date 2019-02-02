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

import static org.apache.hadoop.hbase.backup.util.BackupUtils.succeeded;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.RestoreJob;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.util.Tool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce implementation of {@link RestoreJob}
 *
 * For backup restore, it runs {@link MapReduceHFileSplitterJob} job and creates
 * HFiles which are aligned with a region boundaries of a table being
 * restored.
 *
 * The resulting HFiles then are loaded using HBase bulk load tool {@link BulkLoadHFiles}.
 */
@InterfaceAudience.Private
public class MapReduceRestoreJob implements RestoreJob {
  public static final Logger LOG = LoggerFactory.getLogger(MapReduceRestoreJob.class);

  private Tool player;
  private Configuration conf;

  public MapReduceRestoreJob() {
  }

  @Override
  public void run(Path[] dirPaths, TableName[] tableNames, TableName[] newTableNames,
      boolean fullBackupRestore) throws IOException {
    String bulkOutputConfKey;

    player = new MapReduceHFileSplitterJob();
    bulkOutputConfKey = MapReduceHFileSplitterJob.BULK_OUTPUT_CONF_KEY;
    // Player reads all files in arbitrary directory structure and creates
    // a Map task for each file
    String dirs = StringUtils.join(dirPaths, ",");

    if (LOG.isDebugEnabled()) {
      LOG.debug("Restore " + (fullBackupRestore ? "full" : "incremental")
          + " backup from directory " + dirs + " from hbase tables "
          + StringUtils.join(tableNames, BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND)
          + " to tables "
          + StringUtils.join(newTableNames, BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND));
    }

    for (int i = 0; i < tableNames.length; i++) {
      LOG.info("Restore " + tableNames[i] + " into " + newTableNames[i]);

      Path bulkOutputPath =
          BackupUtils.getBulkOutputDir(BackupUtils.getFileNameCompatibleString(newTableNames[i]),
            getConf());
      Configuration conf = getConf();
      conf.set(bulkOutputConfKey, bulkOutputPath.toString());
      String[] playerArgs = {
        dirs, fullBackupRestore ? newTableNames[i].getNameAsString() : tableNames[i]
              .getNameAsString()
      };

      int result;
      try {

        player.setConf(getConf());
        result = player.run(playerArgs);
        if (succeeded(result)) {
          // do bulk load
          BulkLoadHFiles loader = BackupUtils.createLoader(getConf());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Restoring HFiles from directory " + bulkOutputPath);
          }

          if (loader.bulkLoad(newTableNames[i], bulkOutputPath).isEmpty()) {
            throw new IOException("Can not restore from backup directory " + dirs +
              " (check Hadoop and HBase logs). Bulk loader returns null");
          }
        } else {
          throw new IOException("Can not restore from backup directory " + dirs
              + " (check Hadoop/MR and HBase logs). Player return code =" + result);
        }
        LOG.debug("Restore Job finished:" + result);
      } catch (Exception e) {
        LOG.error(e.toString(), e);
        throw new IOException("Can not restore from backup directory " + dirs
            + " (check Hadoop and HBase logs) ", e);
      }
    }
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
