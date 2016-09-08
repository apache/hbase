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
package org.apache.hadoop.hbase.ipc;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestRpcClientDeprecatedNameMapping {

  @Test
  public void test() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, BlockingRpcClient.class.getName());
    try (RpcClient client = RpcClientFactory.createClient(conf, HConstants.CLUSTER_ID_DEFAULT)) {
      assertThat(client, instanceOf(BlockingRpcClient.class));
    }
    conf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
      "org.apache.hadoop.hbase.ipc.RpcClientImpl");
    try (RpcClient client = RpcClientFactory.createClient(conf, HConstants.CLUSTER_ID_DEFAULT)) {
      assertThat(client, instanceOf(BlockingRpcClient.class));
    }
    conf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, NettyRpcClient.class.getName());
    try (RpcClient client = RpcClientFactory.createClient(conf, HConstants.CLUSTER_ID_DEFAULT)) {
      assertThat(client, instanceOf(NettyRpcClient.class));
    }
    conf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
      "org.apache.hadoop.hbase.ipc.AsyncRpcClient");
    try (RpcClient client = RpcClientFactory.createClient(conf, HConstants.CLUSTER_ID_DEFAULT)) {
      assertThat(client, instanceOf(NettyRpcClient.class));
    }
  }
}
