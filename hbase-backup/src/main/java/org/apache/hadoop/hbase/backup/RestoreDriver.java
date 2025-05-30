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
package org.apache.hadoop.hbase.backup;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Command-line entry point for restore operation
 */
@InterfaceAudience.Private
public class RestoreDriver extends AbstractRestoreDriver {
  private static final String USAGE_STRING = """
      Usage: hbase restore <backup_path> <backup_id> [options]
        backup_path     Path to a backup destination root
        backup_id       Backup image ID to restore
        table(s)        Comma-separated list of tables to restore
      """;

  @Override
  protected int executeRestore(boolean check, TableName[] fromTables, TableName[] toTables,
    boolean isOverwrite) {
    // parse main restore command options
    String[] remainArgs = cmd.getArgs();
    if (remainArgs.length != 2) {
      printToolUsage();
      return -1;
    }

    String backupRootDir = remainArgs[0];
    String backupId = remainArgs[1];

    try (final Connection conn = ConnectionFactory.createConnection(conf);
      BackupAdmin client = new BackupAdminImpl(conn)) {
      client.restore(BackupUtils.createRestoreRequest(backupRootDir, backupId, check, fromTables,
        toTables, isOverwrite));
    } catch (Exception e) {
      LOG.error("Error while running restore backup", e);
      return -5;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Path hbasedir = CommonFSUtils.getRootDir(conf);
    URI defaultFs = hbasedir.getFileSystem(conf).getUri();
    CommonFSUtils.setFsDefault(conf, new Path(defaultFs));
    int ret = ToolRunner.run(conf, new RestoreDriver(), args);
    System.exit(ret);
  }

  @Override
  protected String getUsageString() {
    return USAGE_STRING;
  }
}
