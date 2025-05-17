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

import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.ipc.QosTestBase;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;

@Category({ MasterTests.class, SmallTests.class })
public class TestMasterQosFunction extends QosTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterQosFunction.class);

  private Configuration conf;
  private MasterRpcServices rpcServices;
  private MasterAnnotationReadingPriorityFunction qosFunction;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    rpcServices = Mockito.mock(MasterRpcServices.class);
    when(rpcServices.getConfiguration()).thenReturn(conf);
    qosFunction = new MasterAnnotationReadingPriorityFunction(rpcServices);
  }

  @Test
  public void testRegionInTransition() throws IOException {
    // Check ReportRegionInTransition
    HBaseProtos.RegionInfo meta_ri =
      ProtobufUtil.toRegionInfo(RegionInfoBuilder.FIRST_META_REGIONINFO);
    HBaseProtos.RegionInfo normal_ri =
      ProtobufUtil.toRegionInfo(RegionInfoBuilder.newBuilder(TableName.valueOf("test:table"))
        .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("b")).build());

    RegionServerStatusProtos.RegionStateTransition metaTransition =
      RegionServerStatusProtos.RegionStateTransition.newBuilder().addRegionInfo(meta_ri)
        .setTransitionCode(RegionServerStatusProtos.RegionStateTransition.TransitionCode.CLOSED)
        .build();

    RegionServerStatusProtos.RegionStateTransition normalTransition =
      RegionServerStatusProtos.RegionStateTransition.newBuilder().addRegionInfo(normal_ri)
        .setTransitionCode(RegionServerStatusProtos.RegionStateTransition.TransitionCode.CLOSED)
        .build();

    RegionServerStatusProtos.ReportRegionStateTransitionRequest metaTransitionRequest =
      RegionServerStatusProtos.ReportRegionStateTransitionRequest.newBuilder()
        .setServer(ProtobufUtil.toServerName(ServerName.valueOf("locahost:60020", 100)))
        .addTransition(normalTransition).addTransition(metaTransition).build();

    RegionServerStatusProtos.ReportRegionStateTransitionRequest normalTransitionRequest =
      RegionServerStatusProtos.ReportRegionStateTransitionRequest.newBuilder()
        .setServer(ProtobufUtil.toServerName(ServerName.valueOf("locahost:60020", 100)))
        .addTransition(normalTransition).build();

    final String reportFuncName = "ReportRegionStateTransition";
    checkMethod(conf, reportFuncName, 300, qosFunction, metaTransitionRequest);
    checkMethod(conf, reportFuncName, HConstants.HIGH_QOS, qosFunction, normalTransitionRequest);
  }

  @Test
  public void testAnnotations() {
    checkMethod(conf, "GetLastFlushedSequenceId", HConstants.ADMIN_QOS, qosFunction);
  }

  @Test
  public void testRegionServerStatusProtos() {
    RegionServerStatusProtos.RemoteProcedureResult splitWalProcedureResult =
      RegionServerStatusProtos.RemoteProcedureResult.newBuilder()
        .setStatus(RegionServerStatusProtos.RemoteProcedureResult.Status.SUCCESS).setProcId(100)
        .build();

    RegionServerStatusProtos.ReportProcedureDoneRequest splitWalProcedureDoneReport =
      RegionServerStatusProtos.ReportProcedureDoneRequest.newBuilder()
        .addResult(splitWalProcedureResult).build();

    RegionServerStatusProtos.GetLastFlushedSequenceIdRequest lastFlushedSequenceIdRequest =
      RegionServerStatusProtos.GetLastFlushedSequenceIdRequest.newBuilder()
        .setRegionName(ByteString.copyFrom(RegionInfoBuilder.FIRST_META_REGIONINFO.getRegionName()))
        .build();

    RegionServerStatusProtos.RegionServerReportRequest regionServerReportRequest =
      RegionServerStatusProtos.RegionServerReportRequest.newBuilder()
        .setServer(ProtobufUtil.toServerName(ServerName.valueOf("locahost:60020", 100))).build();

    checkMethod(conf, "ReportProcedureDone", HConstants.HIGH_QOS, qosFunction,
      splitWalProcedureDoneReport);
    checkMethod(conf, "GetLastFlushedSequenceId", HConstants.HIGH_QOS, qosFunction,
      lastFlushedSequenceIdRequest);
    checkMethod(conf, "RegionServerReport", HConstants.HIGH_QOS, qosFunction,
      regionServerReportRequest);
  }
}
