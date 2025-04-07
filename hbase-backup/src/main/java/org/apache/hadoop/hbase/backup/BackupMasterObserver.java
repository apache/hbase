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

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class BackupMasterObserver implements MasterObserver, MasterCoprocessor {

  private final Optional<MasterObserver> observer;

  public BackupMasterObserver() {
    this.observer = Optional.of(this);
  }

  @Override
  public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName) throws IOException {
    MasterCoprocessorEnvironment env = ctx.getEnvironment();
    if (tableName.equals(BackupSystemTable.getTableName(env.getConfiguration()))) {
      return;
    }

    BackupSystemTable table = new BackupSystemTable(env.getConnection());
    try {
      table.startBackupExclusiveOperation();
      table.invalidateTableAncestry(tableName);
    } finally {
      table.finishBackupExclusiveOperation();
    }
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return observer;
  }
}
