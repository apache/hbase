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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.util.Optional;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ModifyTableDescriptorProcedure;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Procedure for setting StoreFileTracker information to table descriptor.
 */
@InterfaceAudience.Private
public class InitializeStoreFileTrackerProcedure extends ModifyTableDescriptorProcedure {

  public InitializeStoreFileTrackerProcedure() {
  }

  public InitializeStoreFileTrackerProcedure(MasterProcedureEnv env, TableName tableName) {
    super(env, tableName);
  }

  @Override
  protected Optional<TableDescriptor> modify(MasterProcedureEnv env, TableDescriptor current) {
    if (StringUtils.isEmpty(current.getValue(StoreFileTrackerFactory.TRACKER_IMPL))) {
      // no tracker impl means it is a table created in previous version, the tracker impl can only
      // be default.
      TableDescriptor td =
        TableDescriptorBuilder.newBuilder(current).setValue(StoreFileTrackerFactory.TRACKER_IMPL,
          StoreFileTrackerFactory.Trackers.DEFAULT.name()).build();
      return Optional.of(td);
    }
    return Optional.empty();
  }
}
