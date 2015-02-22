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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;

/**
 * Action that changes the encoding on a column family from a list of tables.
 */
public class ChangeEncodingAction extends Action {
  private final TableName tableName;

  private Admin admin;
  private Random random;

  public ChangeEncodingAction(TableName tableName) {
    this.tableName = tableName;
    this.random = new Random();
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

    if (columnDescriptors == null || columnDescriptors.length == 0) {
      return;
    }

    LOG.debug("Performing action: Changing encodings on " + tableName);
    // possible DataBlockEncoding id's
    int[] possibleIds = {0, 2, 3, 4, 6};
    for (HColumnDescriptor descriptor : columnDescriptors) {
      short id = (short) possibleIds[random.nextInt(possibleIds.length)];
      descriptor.setDataBlockEncoding(DataBlockEncoding.getEncodingById(id));
      LOG.debug("Set encoding of column family " + descriptor.getNameAsString()
        + " to: " + descriptor.getDataBlockEncoding());
    }

    admin.modifyTable(tableName, tableDescriptor);
  }
}
