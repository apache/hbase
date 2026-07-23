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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * See HBASE-28608. Configuration tests for (non async) connections
 */
@Tag(ClientTests.TAG)
@Tag(SmallTests.TAG)
public class TestConnectionConfiguration {

  @Test
  public void testDefaultMetaOperationTimeout() {
    Configuration conf = HBaseConfiguration.create();
    long clientOperationTimeoutMs = 1000;
    conf.setLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, clientOperationTimeoutMs);
    ConnectionConfiguration config = new ConnectionConfiguration(conf);
    assertEquals(clientOperationTimeoutMs, config.getOperationTimeout());
    assertEquals(clientOperationTimeoutMs, config.getMetaOperationTimeout());
  }

  @Test
  public void testNegativeTimeoutsThrow() {
    List<String> timeoutKeys = Arrays.asList(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT,
      HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, ConnectionConfiguration.PRIMARY_CALL_TIMEOUT_MICROSECOND,
      ConnectionConfiguration.PRIMARY_SCAN_TIMEOUT_MICROSECOND,
      HConstants.HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT, HConstants.HBASE_RPC_TIMEOUT_KEY,
      HConstants.HBASE_RPC_READ_TIMEOUT_KEY, ConnectionConfiguration.HBASE_CLIENT_META_READ_RPC_TIMEOUT_KEY,
      HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY);

    for (String key : timeoutKeys) {
      Configuration conf = HBaseConfiguration.create();
      conf.setInt(key, -1);
      IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> new ConnectionConfiguration(conf), key);
      assertTrue(error.getMessage().contains(key), key);
    }
  }

  @Test
  public void testZeroTimeoutsAreAllowed() {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 0);
    conf.setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 0);
    conf.setInt(ConnectionConfiguration.PRIMARY_CALL_TIMEOUT_MICROSECOND, 0);
    conf.setInt(ConnectionConfiguration.PRIMARY_SCAN_TIMEOUT_MICROSECOND, 0);
    conf.setInt(HConstants.HBASE_CLIENT_META_REPLICA_SCAN_TIMEOUT, 0);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 0);
    conf.setInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, 0);
    conf.setInt(ConnectionConfiguration.HBASE_CLIENT_META_READ_RPC_TIMEOUT_KEY, 0);
    conf.setInt(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY, 0);

    ConnectionConfiguration config = new ConnectionConfiguration(conf);
    assertEquals(0, config.getOperationTimeout());
    assertEquals(0, config.getMetaOperationTimeout());
    assertEquals(0, config.getPrimaryCallTimeoutMicroSecond());
    assertEquals(0, config.getReplicaCallTimeoutMicroSecondScan());
    assertEquals(0, config.getMetaReplicaCallTimeoutMicroSecondScan());
    assertEquals(0, config.getRpcTimeout());
    assertEquals(0, config.getReadRpcTimeout());
    assertEquals(0, config.getMetaReadRpcTimeout());
    assertEquals(0, config.getWriteRpcTimeout());
  }

}
