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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Unit test for Protocol extending common interface. */
@Category(SmallTests.class)
public class TestProtocolExtension {
  private static final String ADDRESS = "0.0.0.0";

  public static final Log LOG =
    LogFactory.getLog(TestProtocolExtension.class);
  
  private static Configuration conf = new Configuration();

  public interface ProtocolExtention {
    void logClassName();
  }

  public interface TestProtocol extends VersionedProtocol, ProtocolExtention {
    public static final long VERSION = 7L;

    void ping() throws IOException;

    // @Override  // Uncomment to make the test pass
    // public void logClassName();
}

  public static class TestImpl implements TestProtocol {
    public long getProtocolVersion(String protocol, long clientVersion) {
      return TestProtocol.VERSION;
    }

    @Override
    public void ping() {}

    @Override
    public void logClassName() {
      LOG.info(this.getClass().getName());
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      return new ProtocolSignature(VERSION, null);
    }
  }

  @Test
  public void testCalls() throws Exception {
    RpcServer server = HBaseRPC.getServer(TestProtocol.class,
                                  new TestImpl(),
                                  new Class<?>[]{ProtocolExtention.class},
                                  ADDRESS,
                                  6016,
                                  10, 10, false,
                                  conf, 10);
    TestProtocol proxy = null;
    try {
      server.start();

      InetSocketAddress addr = server.getListenerAddress();
      proxy = (TestProtocol)HBaseRPC.getProxy(
          TestProtocol.class, TestProtocol.VERSION, addr, conf, 10000);

      proxy.ping();

      proxy.logClassName();
    } finally {
      server.stop();
      if(proxy!=null) HBaseRPC.stopProxy(proxy);
    }
  }
  
  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}
