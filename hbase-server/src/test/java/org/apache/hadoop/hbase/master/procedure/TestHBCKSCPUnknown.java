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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;

import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;


/**
 * Test of the HBCK-version of SCP.
 * The HBCKSCP is an SCP only it reads hbase:meta for list of Regions that were
 * on the server-to-process rather than consult Master in-memory-state.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestHBCKSCPUnknown extends TestHBCKSCP {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBCKSCPUnknown.class);

  @Override
  protected long scheduleHBCKSCP(ServerName rsServerName, HMaster master) throws ServiceException {
    MasterProtos.ScheduleSCPsForUnknownServersResponse response =
        master.getMasterRpcServices().scheduleSCPsForUnknownServers(null,
            MasterProtos.ScheduleSCPsForUnknownServersRequest.newBuilder().build());
    assertEquals(1, response.getPidCount());
    long pid = response.getPid(0);
    return pid;
  }
}
