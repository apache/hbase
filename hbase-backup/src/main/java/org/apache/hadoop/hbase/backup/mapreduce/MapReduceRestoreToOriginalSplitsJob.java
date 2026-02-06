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
package org.apache.hadoop.hbase.backup.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.RestoreJob;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.Tool;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MapReduceRestoreToOriginalSplitsJob implements RestoreJob {
  private Configuration conf;

  @Override
  public void run(Path[] dirPaths, TableName[] fromTables, Path restoreRootDir,
    TableName[] toTables, boolean fullBackupRestore) throws IOException {
    Configuration conf = getConf();
    conf.setBoolean(BulkLoadHFiles.ALWAYS_COPY_FILES, true);

    for (int i = 0; i < fromTables.length; ++i) {
      Path bulkOutputPath = BackupUtils.getBulkOutputDir(restoreRootDir,
        BackupUtils.getFileNameCompatibleString(toTables[i]), getConf());

      for (Path dirPath : dirPaths) {
        String[] args = new String[] { dirPath.toString(), bulkOutputPath.toString() };

        try {
          Tool player = getDistCp(conf, fullBackupRestore);
          int result = player.run(args);

          if (!BackupUtils.succeeded(result)) {
            throw new IOException("DistCp failed with exit code " + result);
          }
        } catch (Exception e) {
          throw new IOException(e);
        }

        BulkLoadHFiles loader = BackupUtils.createLoader(conf);
        loader.bulkLoad(toTables[i], bulkOutputPath);
      }
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private static DistCp getDistCp(Configuration conf, boolean fullBackupRestore) throws Exception {
    if (fullBackupRestore) {
      return new DistCp(conf, null);
    }

    return new IncrementalBackupDistCp(conf);
  }

  private static class IncrementalBackupDistCp extends DistCp {
    public IncrementalBackupDistCp(Configuration conf) throws Exception {
      super(conf, null);
    }

    @Override
    protected Path createInputFileListing(Job job) throws IOException {
      return InputFileListingGenerator.createInputFileListing(this, job, getConf(),
        getFileListingPath(), 2);
    }
  }
}
