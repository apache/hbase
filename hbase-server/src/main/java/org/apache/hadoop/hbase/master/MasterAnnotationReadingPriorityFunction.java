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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.AnnotationReadingPriorityFunction;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;

/**
 * Priority function specifically for the master.
 * <p/>
 * This doesn't make the super users always priority since that would make everything to the master
 * into high priority.
 * <p/>
 * Specifically when reporting that a region is in transition master will try and edit the meta
 * table. That edit will block the thread until successful. However if at the same time meta is also
 * moving then we need to ensure that the regular region that's moving isn't blocking processing of
 * the request to online meta. To accomplish this this priority function makes sure that all
 * requests to transition meta are handled in different threads from other report region in
 * transition calls.
 * <p/>
 * After HBASE-21754, ReportRegionStateTransitionRequest for meta region will be assigned a META_QOS
 * , a separate executor called metaTransitionExecutor will execute it. Other transition request
 * will be executed in priorityExecutor to prevent being mixed with normal requests
 */
@InterfaceAudience.Private
public class MasterAnnotationReadingPriorityFunction
  extends AnnotationReadingPriorityFunction<MasterRpcServices> {

  /**
   * We reference this value in SimpleRpcScheduler so this class have to be public instead of
   * package private
   */
  public static final int META_TRANSITION_QOS = 300;

  MasterAnnotationReadingPriorityFunction(MasterRpcServices rpcServices) {
    super(rpcServices);
  }

  @Override
  protected int normalizePriority(int priority) {
    // no one can have higher priority than meta transition.
    if (priority >= META_TRANSITION_QOS) {
      return META_TRANSITION_QOS - 1;
    } else {
      return priority;
    }
  }

  @Override
  protected int getBasePriority(RequestHeader header, Message param) {
    // If meta is moving then all the other of reports of state transitions will be
    // un able to edit meta. Those blocked reports should not keep the report that opens meta from
    // running. Hence all reports of meta transition should always be in a different thread.
    // This keeps from deadlocking the cluster.
    if (param instanceof RegionServerStatusProtos.ReportRegionStateTransitionRequest) {
      // Regions are moving. Lets see which ones.
      RegionServerStatusProtos.ReportRegionStateTransitionRequest tRequest =
        (RegionServerStatusProtos.ReportRegionStateTransitionRequest) param;
      for (RegionServerStatusProtos.RegionStateTransition rst : tRequest.getTransitionList()) {
        if (rst.getRegionInfoList() != null) {
          for (HBaseProtos.RegionInfo info : rst.getRegionInfoList()) {
            TableName tn = ProtobufUtil.toTableName(info.getTableName());
            if (MetaTableName.getInstance().equals(tn)) {
              return META_TRANSITION_QOS;
            }
          }
        }
      }
      return HConstants.HIGH_QOS;
    }
    // also use HIGH_QOS for all rest methods in RegionServerStatusProtos
    if (RegionServerStatusProtos.class.equals(param.getClass().getEnclosingClass())) {
      return HConstants.HIGH_QOS;
    }
    // Trust the client-set priorities if set
    if (header.hasPriority()) {
      return header.getPriority();
    }
    return HConstants.NORMAL_QOS;
  }

  @Override
  public long getDeadline(RequestHeader header, Message param) {
    return 0;
  }
}
