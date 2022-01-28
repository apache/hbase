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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyColumnFamilyStoreFileTrackerStateData;

@InterfaceAudience.Private
public class ModifyColumnFamilyStoreFileTrackerProcedure extends ModifyStoreFileTrackerProcedure {

  private byte[] family;

  public ModifyColumnFamilyStoreFileTrackerProcedure() {
  }

  public ModifyColumnFamilyStoreFileTrackerProcedure(MasterProcedureEnv env, TableName tableName,
    byte[] family, String dstSFT) throws HBaseIOException {
    super(env, tableName, dstSFT);
    this.family = family;
  }

  @Override
  protected void preCheck(TableDescriptor current) throws IOException {
    if (!current.hasColumnFamily(family)) {
      throw new NoSuchColumnFamilyException(
        Bytes.toStringBinary(family) + " does not exist for table " + current.getTableName());
    }
  }

  @Override
  protected Configuration createConf(Configuration conf, TableDescriptor current) {
    ColumnFamilyDescriptor cfd = current.getColumnFamily(family);
    return StoreUtils.createStoreConfiguration(conf, current, cfd);
  }

  @Override
  protected TableDescriptor createRestoreTableDescriptor(TableDescriptor current,
    String restoreSFT) {
    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(current.getColumnFamily(family))
        .setConfiguration(StoreFileTrackerFactory.TRACKER_IMPL, restoreSFT).build();
    return TableDescriptorBuilder.newBuilder(current).modifyColumnFamily(cfd).build();
  }

  @Override
  protected TableDescriptor createMigrationTableDescriptor(Configuration conf,
    TableDescriptor current) {
    ColumnFamilyDescriptorBuilder builder =
      ColumnFamilyDescriptorBuilder.newBuilder(current.getColumnFamily(family));
    migrate(conf, builder::setConfiguration);
    return TableDescriptorBuilder.newBuilder(current).modifyColumnFamily(builder.build()).build();
  }

  @Override
  protected TableDescriptor createFinishTableDescriptor(TableDescriptor current) {
    ColumnFamilyDescriptorBuilder builder =
      ColumnFamilyDescriptorBuilder.newBuilder(current.getColumnFamily(family));
    finish(builder::setConfiguration, builder::removeConfiguration);
    return TableDescriptorBuilder.newBuilder(current).modifyColumnFamily(builder.build()).build();
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(ModifyColumnFamilyStoreFileTrackerStateData.newBuilder()
      .setFamily(ByteString.copyFrom(family)).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    ModifyColumnFamilyStoreFileTrackerStateData data =
      serializer.deserialize(ModifyColumnFamilyStoreFileTrackerStateData.class);
    this.family = data.getFamily().toByteArray();
  }
}
