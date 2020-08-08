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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action that changes the number of versions on a column family from a list of tables.
 *
 * Always keeps at least 1 as the number of versions.
 */
public class ChangeVersionsAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(ChangeVersionsAction.class);
  private final TableName tableName;
  private final Random random;

  private Admin admin;

  public ChangeVersionsAction(TableName tableName) {
    this.tableName = tableName;
    this.random = new Random();
  }

  @Override protected Logger getLogger() {
    return LOG;
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

    if ( columnDescriptors == null || columnDescriptors.length == 0) {
      return;
    }

    int versions =  random.nextInt(3) + 1;
    for(HColumnDescriptor descriptor:columnDescriptors) {
      descriptor.setVersions(versions, versions);
    }
    // Don't try the modify if we're stopping
    if (context.isStopping()) {
      return;
    }
    getLogger().debug("Performing action: Changing versions on " + tableName.getNameAsString());
    admin.modifyTable(tableName, tableDescriptor);
  }
}
