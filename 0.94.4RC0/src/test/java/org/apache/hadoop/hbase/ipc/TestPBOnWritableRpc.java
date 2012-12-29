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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;

/** Unit tests to test PB-based types on WritableRpcEngine. */
@Category(MediumTests.class)
public class TestPBOnWritableRpc {

  private static Configuration conf = new Configuration();

  public interface TestProtocol extends VersionedProtocol {
    public static final long VERSION = 1L;

    String echo(String value) throws IOException;
    Writable echo(Writable value) throws IOException;

    DescriptorProtos.EnumDescriptorProto exchangeProto(
      DescriptorProtos.EnumDescriptorProto arg);
  }

  public static class TestImpl implements TestProtocol {
    public long getProtocolVersion(String protocol, long clientVersion) {
      return TestProtocol.VERSION;
    }

    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
        int hashcode) {
      return new ProtocolSignature(TestProtocol.VERSION, null);
    }

    @Override
    public String echo(String value) throws IOException { return value; }

    @Override
    public Writable echo(Writable writable) {
      return writable;
    }

    @Override
    public EnumDescriptorProto exchangeProto(EnumDescriptorProto arg) {
      return arg;
    }
  }

  @Test(timeout=10000)
  public void testCalls() throws Exception {
    testCallsInternal(conf);
  }

  private void testCallsInternal(Configuration conf) throws Exception {
    RpcServer rpcServer = HBaseRPC.getServer(new TestImpl(),
      new Class<?>[] {TestProtocol.class},
        "localhost", // BindAddress is IP we got for this server.
        9999, // port number
        2, // number of handlers
        0, // we dont use high priority handlers in master
        conf.getBoolean("hbase.rpc.verbose", false), conf,
        0);
    TestProtocol proxy = null;
    try {
      rpcServer.start();

      InetSocketAddress isa =
        new InetSocketAddress("localhost", 9999);
      proxy = (TestProtocol) HBaseRPC.waitForProxy(
        TestProtocol.class, TestProtocol.VERSION,
        isa, conf, -1, 8000, 8000);

      String stringResult = proxy.echo("foo");
      assertEquals(stringResult, "foo");

      stringResult = proxy.echo((String)null);
      assertEquals(stringResult, null);

      Text utf8Result = (Text)proxy.echo(new Text("hello world"));
      assertEquals(utf8Result, new Text("hello world"));

      utf8Result = (Text)proxy.echo((Text)null);
      assertEquals(utf8Result, null);

      // Test protobufs
      EnumDescriptorProto sendProto =
        EnumDescriptorProto.newBuilder().setName("test").build();
      EnumDescriptorProto retProto = proxy.exchangeProto(sendProto);
      assertEquals(sendProto, retProto);
      assertNotSame(sendProto, retProto);
    } finally {
      rpcServer.stop();
      if(proxy != null) {
        HBaseRPC.stopProxy(proxy);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    new TestPBOnWritableRpc().testCallsInternal(conf);
  }
}
