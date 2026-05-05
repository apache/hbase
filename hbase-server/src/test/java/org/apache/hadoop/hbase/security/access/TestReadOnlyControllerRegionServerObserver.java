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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hbase.WriteAttemptedOnReadOnlyClusterException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

// Tests methods of Region Server Observer which are implemented in ReadOnlyController,
// by mocking the coprocessor environment and dependencies
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
public class TestReadOnlyControllerRegionServerObserver {

  RegionServerReadOnlyController regionServerReadOnlyController;

  // Region Server Coprocessor mocking variables
  ObserverContext<RegionServerCoprocessorEnvironment> ctx;
  AdminProtos.WALEntry walEntry;
  Mutation mutation;

  @BeforeEach
  public void setup() throws Exception {
    regionServerReadOnlyController = new RegionServerReadOnlyController();

    // mocking variables initialization
    ctx = mock(ObserverContext.class);
    walEntry = AdminProtos.WALEntry.newBuilder()
      .setKey(WALProtos.WALKey.newBuilder().setTableName(ByteString.copyFromUtf8("test"))
        .setEncodedRegionName(ByteString.copyFromUtf8("regionA")).setLogSequenceNumber(100)
        .setWriteTime(2).build())
      .build();
    mutation = mock(Mutation.class);
  }

  @AfterEach
  public void tearDown() throws Exception {

  }

  @Test
  public void testPreRollWALWriterRequestReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class,
      () -> regionServerReadOnlyController.preRollWALWriterRequest(ctx));
  }

  @Test
  public void testPreReplicationSinkBatchMutateReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class,
      () -> regionServerReadOnlyController.preReplicationSinkBatchMutate(ctx, walEntry, mutation));
  }

  @Test
  public void testPreReplicateLogEntriesReadOnlyException() {
    assertThrows(WriteAttemptedOnReadOnlyClusterException.class,
      () -> regionServerReadOnlyController.preReplicateLogEntries(ctx));
  }
}
