/**
 *
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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.TableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterServices;

/**
 * Handles adding a new family to an existing table.
 */
@InterfaceAudience.Private
public class TableAddFamilyHandler extends TableEventHandler {

  private final HColumnDescriptor familyDesc;

  public TableAddFamilyHandler(TableName tableName, HColumnDescriptor familyDesc,
      Server server, final MasterServices masterServices) {
    super(EventType.C_M_ADD_FAMILY, tableName, server, masterServices);
    this.familyDesc = familyDesc;
  }

  @Override
  protected void prepareWithTableLock() throws IOException {
    super.prepareWithTableLock();
    TableDescriptor htd = getTableDescriptor();
    if (htd.getHTableDescriptor().hasFamily(familyDesc.getName())) {
      throw new InvalidFamilyOperationException("Family '" +
        familyDesc.getNameAsString() + "' already exists so cannot be added");
    }
  }

  @Override
  protected void handleTableOperation(List<HRegionInfo> hris)
  throws IOException {
    MasterCoprocessorHost cpHost = ((HMaster) this.server)
        .getMasterCoprocessorHost();
    if(cpHost != null){
      cpHost.preAddColumnHandler(this.tableName, this.familyDesc);
    }
    // Update table descriptor
    this.masterServices.getMasterFileSystem().addColumn(tableName, familyDesc);
    if(cpHost != null){
      cpHost.postAddColumnHandler(this.tableName, this.familyDesc);
    }
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    String family = "UnknownFamily";
    if(familyDesc != null) {
      family = familyDesc.getNameAsString();
    }
    return getClass().getSimpleName() + "-" + name + "-" +
                              getSeqid() + "-" + tableName + "-" + family;
  }

}
