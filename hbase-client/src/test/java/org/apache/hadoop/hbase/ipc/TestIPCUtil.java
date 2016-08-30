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

import static org.apache.hadoop.hbase.ipc.IPCUtil.wrapException;
import static org.junit.Assert.assertTrue;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;

import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.junit.Test;

public class TestIPCUtil {

  @Test
  public void testWrapException() throws Exception {
    final InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 0);
    assertTrue(wrapException(address, new ConnectException()) instanceof ConnectException);
    assertTrue(
      wrapException(address, new SocketTimeoutException()) instanceof SocketTimeoutException);
    assertTrue(wrapException(address, new ConnectionClosingException(
        "Test AbstractRpcClient#wrapException")) instanceof ConnectionClosingException);
    assertTrue(
      wrapException(address, new CallTimeoutException("Test AbstractRpcClient#wrapException"))
          .getCause() instanceof CallTimeoutException);
  }
}
