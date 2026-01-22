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
package org.apache.hadoop.hbase.security.access;

import static org.apache.hadoop.hbase.HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

// Tests methods of Region Server Observer which are implemented in ReadOnlyController,
// by mocking the coprocessor environment and dependencies
@Category({ SecurityTests.class, SmallTests.class })
public class TestReadOnlyControllerRegionServerObserver {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReadOnlyControllerRegionServerObserver.class);

  RegionServerReadOnlyController regionServerReadOnlyController;
  HBaseConfiguration readOnlyConf;

  // Region Server Coprocessor mocking variables
  ObserverContext<RegionServerCoprocessorEnvironment> ctx;
  AdminProtos.WALEntry walEntry;
  Mutation mutation;

  @Before
  public void setup() throws Exception {
    regionServerReadOnlyController = new RegionServerReadOnlyController();
    readOnlyConf = new HBaseConfiguration();
    readOnlyConf.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);

    // mocking variables initialization
    ctx = mock(ObserverContext.class);
    walEntry = AdminProtos.WALEntry.newBuilder()
      .setKey(WALProtos.WALKey.newBuilder().setTableName(ByteString.copyFromUtf8("test"))
        .setEncodedRegionName(ByteString.copyFromUtf8("regionA")).setLogSequenceNumber(100)
        .setWriteTime(2).build())
      .build();
    mutation = mock(Mutation.class);
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreRollWALWriterRequestReadOnlyException() throws IOException {
    regionServerReadOnlyController.onConfigurationChange(readOnlyConf);
    regionServerReadOnlyController.preRollWALWriterRequest(ctx);
  }

  @Test
  public void testPreRollWALWriterRequestNoException() throws IOException {
    regionServerReadOnlyController.preRollWALWriterRequest(ctx);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreReplicationSinkBatchMutateReadOnlyException() throws IOException {
    regionServerReadOnlyController.onConfigurationChange(readOnlyConf);
    regionServerReadOnlyController.preReplicationSinkBatchMutate(ctx, walEntry, mutation);
  }

  @Test
  public void testPreReplicationSinkBatchMutateNoException() throws IOException {
    regionServerReadOnlyController.preReplicationSinkBatchMutate(ctx, walEntry, mutation);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPreReplicateLogEntriesReadOnlyException() throws IOException {
    regionServerReadOnlyController.onConfigurationChange(readOnlyConf);
    regionServerReadOnlyController.preReplicateLogEntries(ctx);
  }

  @Test
  public void testPreReplicateLogEntriesNoException() throws IOException {
    regionServerReadOnlyController.preReplicateLogEntries(ctx);
  }
}
