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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;

/**
 * A bad Master Observer to prevent user to create/delete table once.
 */
public class BadMasterObserverForCreateDeleteTable implements MasterObserver, MasterCoprocessor {
  private boolean createFailedOnce = false;
  private boolean deleteFailedOnce = false;

  @Override
  public void postCompletedCreateTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableDescriptor desc, RegionInfo[] regions) throws IOException {
    if (!createFailedOnce && !desc.getTableName().isSystemTable()) {
      createFailedOnce = true;
      throw new IOException("execute postCompletedCreateTableAction failed once.");
    }
  }

  @Override
  public void postCompletedDeleteTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
    TableName tableName) throws IOException {
    if (!deleteFailedOnce && !tableName.isSystemTable()) {
      deleteFailedOnce = true;
      throw new IOException("execute postCompletedDeleteTableAction failed once.");
    }
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }
}
