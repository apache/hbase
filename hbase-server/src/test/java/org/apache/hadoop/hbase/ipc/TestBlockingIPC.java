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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ RPCTests.class, MediumTests.class })
public class TestBlockingIPC extends AbstractTestIPC {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBlockingIPC.class);

  @Parameters(name = "{index}: rpcServerImpl={0}")
  public static List<Object[]> data() {
    return Arrays.asList(new Object[] { SimpleRpcServer.class },
      new Object[] { NettyRpcServer.class });
  }

  @Override
  protected BlockingRpcClient createRpcClientNoCodec(Configuration conf) {
    return new BlockingRpcClient(conf) {
      @Override
      protected Codec getCodec() {
        return null;
      }
    };
  }

  @Override
  protected BlockingRpcClient createRpcClient(Configuration conf) {
    return new BlockingRpcClient(conf);
  }

  @Override
  protected BlockingRpcClient createRpcClientRTEDuringConnectionSetup(Configuration conf)
    throws IOException {
    return new BlockingRpcClient(conf) {

      @Override
      protected boolean isTcpNoDelay() {
        throw new RuntimeException("Injected fault");
      }
    };
  }

  @Override
  protected AbstractRpcClient<?> createBadAuthRpcClient(Configuration conf) {
    return new BlockingRpcClient(conf) {

      @Override
      protected BlockingRpcConnection createConnection(ConnectionId remoteId) throws IOException {
        return new BlockingRpcConnection(this, remoteId) {
          @Override
          protected byte[] getConnectionHeaderPreamble() {
            byte[] header = super.getConnectionHeaderPreamble();
            // set an invalid auth code
            header[header.length - 1] = -10;
            return header;
          }
        };
      }
    };
  }
}
