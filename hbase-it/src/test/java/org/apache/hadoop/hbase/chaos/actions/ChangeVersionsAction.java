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
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.TableName;
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

  public ChangeVersionsAction(TableName tableName) {
    this.tableName = tableName;
  }

  @Override protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws IOException {
    final int versions =  ThreadLocalRandom.current().nextInt(3) + 1;
    getLogger().debug("Performing action: Changing versions on " + tableName + " to " + versions);
    modifyAllTableColumns(tableName, columnBuilder -> {
      columnBuilder.setMinVersions(versions).setMaxVersions(versions);
    });
    getLogger().debug("Performing action: Just changed versions on " + tableName);
  }
}
