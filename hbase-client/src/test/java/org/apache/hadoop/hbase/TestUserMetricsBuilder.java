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
package org.apache.hadoop.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;

@Tag(ClientTests.TAG)
@Tag(SmallTests.TAG)
public class TestUserMetricsBuilder {

  @Test
  public void testRoundTripPreservesClientConnectionFields() {
    // Build a user metrics snapshot with all client-connection identity fields populated.
    UserMetrics userMetrics = UserMetricsBuilder.newBuilder(Bytes.toBytes("alice"))
      .addClientMetris(new UserMetricsBuilder.ClientMetricsImpl("clientHost", 11L, 22L, 3L,
        "10.1.1.1", "alice", "ClientService", "1.2.3"))
      .build();

    // Convert to protobuf and back; UI/server paths rely on this round-trip preserving fields.
    ClusterStatusProtos.UserLoad userLoad = UserMetricsBuilder.toUserMetrics(userMetrics);
    UserMetrics converted = UserMetricsBuilder.toUserMetrics(userLoad);

    UserMetrics.ClientMetrics clientMetrics = converted.getClientMetrics().get("clientHost");
    assertNotNull(clientMetrics);
    assertEquals("10.1.1.1", clientMetrics.getHostAddress());
    assertEquals("alice", clientMetrics.getUserName());
    assertEquals("ClientService", clientMetrics.getServiceName());
    assertEquals("1.2.3", clientMetrics.getClientVersion());
  }
}
