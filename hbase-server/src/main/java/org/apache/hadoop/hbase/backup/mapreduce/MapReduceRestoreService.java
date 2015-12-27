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
package org.apache.hadoop.hbase.backup.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.IncrementalRestoreService;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.mapreduce.WALPlayer;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MapReduceRestoreService implements IncrementalRestoreService {
  public static final Log LOG = LogFactory.getLog(MapReduceRestoreService.class);

  private WALPlayer player;

  public MapReduceRestoreService() {
    this.player = new WALPlayer();
  }

  @Override
  public void run(String logDir, String[] tableNames, String[] newTableNames) throws IOException {
    String tableStr = HBackupFileSystem.join(tableNames);
    String newTableStr = HBackupFileSystem.join(newTableNames);

    // WALPlayer reads all files in arbitrary directory structure and creates a Map task for each
    // log file

    String[] playerArgs = { logDir, tableStr, newTableStr };
    LOG.info("Restore incremental backup from directory " + logDir + " from hbase tables "
        + HBackupFileSystem.join(tableNames) + " to tables "
        + HBackupFileSystem.join(newTableNames));
    try {
      player.run(playerArgs);
    } catch (Exception e) {
      throw new IOException("cannot restore from backup directory " + logDir
        + " (check Hadoop and HBase logs) " + e);
    }
  }

  @Override
  public Configuration getConf() {
    return player.getConf();
  }

  @Override
  public void setConf(Configuration conf) {
    this.player.setConf(conf);
  }

}
