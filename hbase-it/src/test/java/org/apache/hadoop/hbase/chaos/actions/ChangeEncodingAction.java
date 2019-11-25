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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action that changes the encoding on a column family from a list of tables.
 */
public class ChangeEncodingAction extends Action {
  private final TableName tableName;
  private final Random random;
  private static final Logger LOG = LoggerFactory.getLogger(ChangeEncodingAction.class);

  public ChangeEncodingAction(TableName tableName) {
    this.tableName = tableName;
    this.random = new Random();
  }

  @Override
  public void perform() throws IOException {
    LOG.debug("Performing action: Changing encodings on " + tableName);
    // possible DataBlockEncoding id's
    final int[] possibleIds = {0, 2, 3, 4, 6};

    modifyAllTableColumns(tableName, (columnName, columnBuilder) -> {
      short id = (short) possibleIds[random.nextInt(possibleIds.length)];
      DataBlockEncoding encoding = DataBlockEncoding.getEncodingById(id);
      columnBuilder.setDataBlockEncoding(encoding);
      LOG.debug("Set encoding of column family " + columnName + " to: " + encoding);
    });
  }
}
