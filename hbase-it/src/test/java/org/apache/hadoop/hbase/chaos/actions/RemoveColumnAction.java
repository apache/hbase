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

package org.apache.hadoop.hbase.chaos.actions;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Action that removes a column family.
 */
public class RemoveColumnAction extends Action {
  private final byte[] tableName;
  private final Set<String> protectedColumns;
  private final String tableNameString;
  private HBaseAdmin admin;
  private Random random;

  public RemoveColumnAction(String tableName, Set<String> protectedColumns) {
    tableNameString = tableName;
    this.tableName = Bytes.toBytes(tableName);
    this.protectedColumns = protectedColumns;
    random = new Random();
  }

  @Override
  public void init(ActionContext context) throws IOException {
    super.init(context);
    this.admin = context.getHBaseIntegrationTestingUtility().getHBaseAdmin();
  }

  @Override
  public void perform() throws Exception {
    HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
    HColumnDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();

    if (columnDescriptors.length <= 1) {
      return;
    }

    int index = random.nextInt(columnDescriptors.length);
    while(protectedColumns != null &&
          protectedColumns.contains(columnDescriptors[index].getNameAsString())) {
      index = random.nextInt(columnDescriptors.length);
    }
    LOG.debug("Performing action: Removing " + columnDescriptors[index].getName() + " from "
        + tableNameString);
    tableDescriptor.removeFamily(columnDescriptors[index].getName());

    admin.modifyTable(tableName, tableDescriptor);
  }
}
