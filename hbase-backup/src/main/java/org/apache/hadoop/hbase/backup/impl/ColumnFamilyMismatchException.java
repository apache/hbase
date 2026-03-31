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
package org.apache.hadoop.hbase.backup.impl;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public final class ColumnFamilyMismatchException extends HBaseIOException {
  private final List<TableName> mismatchedTables;

  private ColumnFamilyMismatchException(String msg, List<TableName> mismatchedTables) {
    super(msg);
    this.mismatchedTables = mismatchedTables;
  }

  public static final class ColumnFamilyMismatchExceptionBuilder {
    private final List<TableName> mismatchedTables = new ArrayList<>();
    private final StringBuilder msg = new StringBuilder();

    public ColumnFamilyMismatchExceptionBuilder addMismatchedTable(TableName tableName,
      ColumnFamilyDescriptor[] currentCfs, ColumnFamilyDescriptor[] backupCfs) {
      this.mismatchedTables.add(tableName);

      String currentCfsParsed = StringUtils.join(currentCfs, ',');
      String backupCfsParsed = StringUtils.join(backupCfs, ',');
      msg.append("\nMismatch in column family descriptor for table: ").append(tableName)
        .append("\n");
      msg.append("Current families: ").append(currentCfsParsed).append("\n");
      msg.append("Backup families: ").append(backupCfsParsed);

      return this;
    }

    public ColumnFamilyMismatchException build() {
      return new ColumnFamilyMismatchException(msg.toString(), mismatchedTables);
    }
  }

  public List<TableName> getMismatchedTables() {
    return mismatchedTables;
  }

  public static ColumnFamilyMismatchExceptionBuilder newBuilder() {
    return new ColumnFamilyMismatchExceptionBuilder();
  }
}
