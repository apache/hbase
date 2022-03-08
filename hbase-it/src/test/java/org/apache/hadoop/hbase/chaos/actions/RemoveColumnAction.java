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

package org.apache.hadoop.hbase.chaos.actions;

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action that removes a column family.
 */
public class RemoveColumnAction extends Action {
  private static final Logger LOG =
      LoggerFactory.getLogger(RemoveColumnAction.class);
  private final TableName tableName;
  private final Set<String> protectedColumns;
  private Admin admin;

  public RemoveColumnAction(TableName tableName, Set<String> protectedColumns) {
    this.tableName = tableName;
    this.protectedColumns = protectedColumns;
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void init(ActionContext context) throws IOException {
    super.init(context);
    this.admin = context.getHBaseIntegrationTestingUtility().getAdmin();
  }

  @Override
  public void perform() throws Exception {
    TableDescriptor tableDescriptor = admin.getDescriptor(tableName);
    ColumnFamilyDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();
    Random rand = ThreadLocalRandom.current();

    if (columnDescriptors.length <= (protectedColumns == null ? 1 : protectedColumns.size())) {
      return;
    }
    int index = rand.nextInt(columnDescriptors.length);
    while(protectedColumns != null &&
          protectedColumns.contains(columnDescriptors[index].getNameAsString())) {
      index = rand.nextInt(columnDescriptors.length);
    }
    byte[] colDescName = columnDescriptors[index].getName();
    getLogger().debug("Performing action: Removing " + Bytes.toString(colDescName)+ " from "
        + tableName.getNameAsString());

    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableDescriptor);
    builder.removeColumnFamily(colDescName);

    // Don't try the modify if we're stopping
    if (context.isStopping()) {
      return;
    }
    admin.modifyTable(builder.build());
  }
}
