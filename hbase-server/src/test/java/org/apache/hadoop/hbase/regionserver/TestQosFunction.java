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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.protobuf.Message;

import java.io.IOException;

/**
 * Basic test that qos function is sort of working; i.e. a change in method naming style
 * over in pb doesn't break it.
 */
@Category(SmallTests.class)
public class TestQosFunction {
  @Test
  public void testPriority() {
    Configuration conf = HBaseConfiguration.create();
    RSRpcServices rpcServices = Mockito.mock(RSRpcServices.class);
    when(rpcServices.getConfiguration()).thenReturn(conf);

    AnnotationReadingPriorityFunction qosFunction =
      new AnnotationReadingPriorityFunction(rpcServices, RSRpcServices.class);

    // Set method name in pb style with the method name capitalized.
    checkMethod(conf, "ReplicateWALEntry", HConstants.REPLICATION_QOS, qosFunction);
    // Set method name in pb style with the method name capitalized.
    checkMethod(conf, "OpenRegion", HConstants.ADMIN_QOS, qosFunction);
    // Check multi works.
    checkMethod(conf, "Multi", HConstants.NORMAL_QOS, qosFunction,
        MultiRequest.getDefaultInstance());

  }

  @Test
  public void testRegionInTransition() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    Superusers.initialize(conf);
    RSRpcServices rpcServices = Mockito.mock(RSRpcServices.class);
    when(rpcServices.getConfiguration()).thenReturn(conf);

    AnnotationReadingPriorityFunction qosFunction =
        new AnnotationReadingPriorityFunction(rpcServices, RSRpcServices.class);

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

  private void checkMethod(Configuration conf, final String methodName, final int expected,
      final AnnotationReadingPriorityFunction qosf) {
    checkMethod(conf, methodName, expected, qosf, null);
  }

  private void checkMethod(Configuration conf, final String methodName, final int expected,
      final AnnotationReadingPriorityFunction qosf, final Message param) {
    RequestHeader.Builder builder = RequestHeader.newBuilder();
    builder.setMethodName(methodName);
    assertEquals(methodName, expected, qosf.getPriority(builder.build(), param,
      User.createUserForTesting(conf, "someuser", new String[]{"somegroup"})));
  }
}
