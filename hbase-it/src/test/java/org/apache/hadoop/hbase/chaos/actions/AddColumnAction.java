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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action the adds a column family to a table.
 */
public class AddColumnAction extends Action {
  private final TableName tableName;
  private Admin admin;
  private static final Logger LOG = LoggerFactory.getLogger(AddColumnAction.class);

  public AddColumnAction(TableName tableName) {
    this.tableName = tableName;
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
    ColumnFamilyDescriptor columnDescriptor = null;

    while (columnDescriptor == null
        || tableDescriptor.getColumnFamily(columnDescriptor.getName()) != null) {
      columnDescriptor = ColumnFamilyDescriptorBuilder.of(RandomStringUtils.randomAlphabetic(5));
    }

    // Don't try the modify if we're stopping
    if (context.isStopping()) {
      return;
    }

    getLogger().debug("Performing action: Adding " + columnDescriptor + " to " + tableName);

    TableDescriptor modifiedTable = TableDescriptorBuilder.newBuilder(tableDescriptor)
        .setColumnFamily(columnDescriptor).build();
    admin.modifyTable(modifiedTable);
  }
}
