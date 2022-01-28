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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ModifyTableStoreFileTrackerProcedure extends ModifyStoreFileTrackerProcedure {

  public ModifyTableStoreFileTrackerProcedure() {
  }

  public ModifyTableStoreFileTrackerProcedure(MasterProcedureEnv env, TableName tableName,
    String dstSFT) throws HBaseIOException {
    super(env, tableName, dstSFT);
  }

  @Override
  protected void preCheck(TableDescriptor current) {
  }

  @Override
  protected Configuration createConf(Configuration conf, TableDescriptor current) {
    return new CompoundConfiguration().add(conf).addBytesMap(current.getValues());
  }

  @Override
  protected TableDescriptor createRestoreTableDescriptor(TableDescriptor current,
    String restoreSFT) {
    return TableDescriptorBuilder.newBuilder(current)
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL, restoreSFT).build();
  }

  @Override
  protected TableDescriptor createMigrationTableDescriptor(Configuration conf,
    TableDescriptor current) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(current);
    migrate(conf, builder::setValue);
    return builder.build();
  }

  @Override
  protected TableDescriptor createFinishTableDescriptor(TableDescriptor current) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(current);
    finish(builder::setValue, builder::removeValue);
    return builder.build();
  }

}
