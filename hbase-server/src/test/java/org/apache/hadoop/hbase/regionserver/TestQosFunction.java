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

import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.QosTestHelper;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;

/**
 * Basic test that qos function is sort of working; i.e. a change in method naming style
 * over in pb doesn't break it.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestQosFunction extends QosTestHelper {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestQosFunction.class);

  private Configuration conf;
  private RSRpcServices rpcServices;
  private AnnotationReadingPriorityFunction qosFunction;


  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    rpcServices = Mockito.mock(RSRpcServices.class);
    when(rpcServices.getConfiguration()).thenReturn(conf);
    qosFunction = new AnnotationReadingPriorityFunction(rpcServices, RSRpcServices.class);
  }

  @Test
  public void testPriority() {
    // Set method name in pb style with the method name capitalized.
    checkMethod(conf, "ReplicateWALEntry", HConstants.REPLICATION_QOS, qosFunction);
    // Set method name in pb style with the method name capitalized.
    checkMethod(conf, "OpenRegion", HConstants.ADMIN_QOS, qosFunction);
    // Check multi works.
    checkMethod(conf, "Multi", HConstants.NORMAL_QOS, qosFunction,
        MultiRequest.getDefaultInstance());

  }


  @Test
  public void testAnnotations() {
    checkMethod(conf, "CloseRegion", HConstants.ADMIN_QOS, qosFunction);
    checkMethod(conf, "CompactRegion", HConstants.ADMIN_QOS, qosFunction);
    checkMethod(conf, "FlushRegion", HConstants.ADMIN_QOS, qosFunction);
  }
}
