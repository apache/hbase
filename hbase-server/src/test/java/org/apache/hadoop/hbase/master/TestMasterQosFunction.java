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

import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.QosTestHelper;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.regionserver.AnnotationReadingPriorityFunction;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({MasterTests.class, SmallTests.class})
public class TestMasterQosFunction extends QosTestHelper {
  private Configuration conf;
  private RSRpcServices rpcServices;
  private AnnotationReadingPriorityFunction qosFunction;


  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    rpcServices = Mockito.mock(MasterRpcServices.class);
    when(rpcServices.getConfiguration()).thenReturn(conf);
    qosFunction = new MasterAnnotationReadingPriorityFunction(rpcServices, MasterRpcServices.class);
  }

  @Test
  public void testRegionInTransition() throws IOException {
    // Check ReportRegionInTransition
    HBaseProtos.RegionInfo meta_ri = HRegionInfo.convert(HRegionInfo.FIRST_META_REGIONINFO);
    HBaseProtos.RegionInfo normal_ri = HRegionInfo.convert(
        new HRegionInfo(TableName.valueOf("test:table"),
            Bytes.toBytes("a"), Bytes.toBytes("b"), false));


    RegionServerStatusProtos.RegionStateTransition metaTransition = RegionServerStatusProtos
        .RegionStateTransition.newBuilder()
        .addRegionInfo(meta_ri)
        .setTransitionCode(RegionServerStatusProtos.RegionStateTransition.TransitionCode.CLOSED)
        .build();

    RegionServerStatusProtos.RegionStateTransition normalTransition = RegionServerStatusProtos
        .RegionStateTransition.newBuilder()
        .addRegionInfo(normal_ri)
        .setTransitionCode(RegionServerStatusProtos.RegionStateTransition.TransitionCode.CLOSED)
        .build();

    RegionServerStatusProtos.ReportRegionStateTransitionRequest metaTransitionRequest =
        RegionServerStatusProtos.ReportRegionStateTransitionRequest.newBuilder()
            .setServer(ProtobufUtil.toServerName(ServerName.valueOf("locahost:60020", 100)))
            .addTransition(normalTransition)
            .addTransition(metaTransition).build();

    RegionServerStatusProtos.ReportRegionStateTransitionRequest normalTransitionRequest =
        RegionServerStatusProtos.ReportRegionStateTransitionRequest.newBuilder()
            .setServer(ProtobufUtil.toServerName(ServerName.valueOf("locahost:60020", 100)))
            .addTransition(normalTransition).build();

    final String reportFuncName = "ReportRegionStateTransition";
    checkMethod(conf, reportFuncName, HConstants.SYSTEMTABLE_QOS, qosFunction,
        metaTransitionRequest);
    checkMethod(conf, reportFuncName, HConstants.NORMAL_QOS, qosFunction, normalTransitionRequest);
  }

  @Test
  public void testAnnotations() {
    checkMethod(conf, "GetLastFlushedSequenceId", HConstants.ADMIN_QOS, qosFunction);
    checkMethod(conf, "CompactRegion", HConstants.ADMIN_QOS, qosFunction);
    checkMethod(conf, "GetLastFlushedSequenceId", HConstants.ADMIN_QOS, qosFunction);
    checkMethod(conf, "GetRegionInfo", HConstants.ADMIN_QOS, qosFunction);
  }
}