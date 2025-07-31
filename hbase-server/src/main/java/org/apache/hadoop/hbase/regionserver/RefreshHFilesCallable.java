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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;

@InterfaceAudience.Private
public class RefreshHFilesCallable extends BaseRSProcedureCallable {
  private static final Logger LOG = LoggerFactory.getLogger(RefreshHFilesCallable.class);

  private RegionInfo regionInfo;

  @Override
  protected void doCall() throws Exception {
    HRegion region = rs.getRegion(regionInfo.getEncodedName());
    LOG.debug("Starting refreshHfiles operation on region {}", region);

    try {
      for (Store store : region.getStores()) {
        store.refreshStoreFiles();
      }
    } catch (IOException ioe) {
      LOG.warn("Exception while trying to refresh store files: ", ioe);
    }
  }

  @Override
  protected void initParameter(byte[] parameter) throws Exception {
    MasterProcedureProtos.RefreshHFilesRegionParameter param =
      MasterProcedureProtos.RefreshHFilesRegionParameter.parseFrom(parameter);
    this.regionInfo = ProtobufUtil.toRegionInfo(param.getRegion());
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_REFRESH_HFILES;
  }
}
