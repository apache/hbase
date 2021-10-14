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
package org.apache.hadoop.hbase.rsgroup;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ModifyTableDescriptorProcedure;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Procedure for migrating rs group information to table descriptor.
 */
@InterfaceAudience.Private
public class MigrateRSGroupProcedure extends ModifyTableDescriptorProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(MigrateRSGroupProcedure.class);

  public MigrateRSGroupProcedure() {
  }

  public MigrateRSGroupProcedure(MasterProcedureEnv env, TableDescriptor unmodified) {
    super(env, unmodified);
  }

  @Override
  protected Optional<TableDescriptor> modify(MasterProcedureEnv env, TableDescriptor current)
    throws IOException {
    if (current.getRegionServerGroup().isPresent()) {
      // usually this means user has set the rs group using the new code which will set the group
      // directly on table descriptor, skip.
      LOG.debug("Skip migrating {} since it is already in group {}", current.getTableName(),
        current.getRegionServerGroup().get());
      return Optional.empty();
    }
    RSGroupInfo group =
      env.getMasterServices().getRSGroupInfoManager().getRSGroupForTable(current.getTableName());
    if (group == null) {
      LOG.debug("RSGroup for table {} is empty when migrating, usually this should not happen" +
        " unless we have removed the RSGroup, ignore...", current.getTableName());
      return Optional.empty();
    }
    return Optional
      .of(TableDescriptorBuilder.newBuilder(current).setRegionServerGroup(group.getName()).build());
  }
}
