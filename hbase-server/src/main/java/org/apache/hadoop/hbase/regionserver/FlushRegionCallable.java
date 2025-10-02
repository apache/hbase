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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.FlushRegionParameter;

@InterfaceAudience.Private
public class FlushRegionCallable extends BaseRSProcedureCallable {

  private static final Logger LOG = LoggerFactory.getLogger(FlushRegionCallable.class);

  private RegionInfo regionInfo;

  private List<byte[]> columnFamilies;

  @Override
  protected byte[] doCall() throws Exception {
    HRegion region = rs.getRegion(regionInfo.getEncodedName());
    if (region == null) {
      throw new NotServingRegionException("region=" + regionInfo.getRegionNameAsString());
    }
    LOG.debug("Starting region operation on {}", region);
    region.startRegionOperation();
    try {
      HRegion.FlushResult res;
      if (columnFamilies == null) {
        res = region.flush(true);
      } else {
        res = region.flushcache(columnFamilies, false, FlushLifeCycleTracker.DUMMY);
      }
      if (res.getResult() == HRegion.FlushResult.Result.CANNOT_FLUSH) {
        throw new IOException("Unable to complete flush " + regionInfo);
      }
    } finally {
      LOG.debug("Closing region operation on {}", region);
      region.closeRegionOperation();
    }
    return null;
  }

  @Override
  protected void initParameter(byte[] parameter) throws Exception {
    FlushRegionParameter param = FlushRegionParameter.parseFrom(parameter);
    this.regionInfo = ProtobufUtil.toRegionInfo(param.getRegion());
    if (param.getColumnFamilyCount() > 0) {
      this.columnFamilies = param.getColumnFamilyList().stream().filter(cf -> !cf.isEmpty())
        .map(ByteString::toByteArray).collect(Collectors.toList());
    }
  }

  @Override
  public EventType getEventType() {
    return EventType.RS_FLUSH_REGIONS;
  }
}
