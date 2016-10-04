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
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.regionserver.AnnotationReadingPriorityFunction;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.security.User;

/**
 * Priority function specifically for the master.
 *
 * This doesn't make the super users always priority since that would make everything
 * to the master into high priority.
 *
 * Specifically when reporting that a region is in transition master will try and edit the meta
 * table. That edit will block the thread until successful. However if at the same time meta is
 * also moving then we need to ensure that the regular region that's moving isn't blocking
 * processing of the request to online meta. To accomplish this this priority function makes sure
 * that all requests to transition meta are handled in different threads from other report region
 * in transition calls.
 */
public class MasterAnnotationReadingPriorityFunction extends AnnotationReadingPriorityFunction {
  public MasterAnnotationReadingPriorityFunction(final RSRpcServices rpcServices) {
    this(rpcServices, rpcServices.getClass());
  }


  public MasterAnnotationReadingPriorityFunction(RSRpcServices rpcServices,
                                          Class<? extends RSRpcServices> clz) {
    super(rpcServices, clz);
  }

  public int getPriority(RPCProtos.RequestHeader header, Message param, User user) {
    // Yes this is copy pasted from the base class but it keeps from having to look in the
    // annotatedQos table twice something that could get costly since this is called for
    // every single RPC request.
    int priorityByAnnotation = getAnnotatedPriority(header);
    if (priorityByAnnotation >= 0) {
      return priorityByAnnotation;
    }

    // If meta is moving then all the other of reports of state transitions will be
    // un able to edit meta. Those blocked reports should not keep the report that opens meta from
    // running. Hence all reports of meta transitioning should always be in a different thread.
    // This keeps from deadlocking the cluster.
    if (param instanceof RegionServerStatusProtos.ReportRegionStateTransitionRequest) {
      // Regions are moving. Lets see which ones.
      RegionServerStatusProtos.ReportRegionStateTransitionRequest
          tRequest = (RegionServerStatusProtos.ReportRegionStateTransitionRequest) param;
      for (RegionServerStatusProtos.RegionStateTransition rst : tRequest.getTransitionList()) {
        if (rst.getRegionInfoList() != null) {
          for (HBaseProtos.RegionInfo info : rst.getRegionInfoList()) {
            TableName tn = ProtobufUtil.toTableName(info.getTableName());
            if (tn.isSystemTable()) {
              return HConstants.SYSTEMTABLE_QOS;
            }
          }
        }
      }
      return HConstants.NORMAL_QOS;
    }

    // Handle the rest of the different reasons to change priority.
    return getBasePriority(header, param);
  }
}
