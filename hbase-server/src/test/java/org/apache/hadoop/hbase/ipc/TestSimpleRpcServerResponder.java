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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag(RPCTests.TAG)
@Tag(SmallTests.TAG)
public class TestSimpleRpcServerResponder {

  private SimpleRpcServerResponder responder;
  private SocketChannel registeredChannel;

  @AfterEach
  public void tearDown() throws Exception {
    if (responder != null) {
      writeSelector().close();
    }
    if (registeredChannel != null) {
      registeredChannel.close();
    }
  }

  @Test
  public void testCompletedPartialResponseClearsLastSentTime() throws Exception {
    SimpleRpcServer rpcServer = mockRpcServer();
    responder = new SimpleRpcServerResponder(rpcServer);
    SimpleServerRpcConnection connection = newConnection(rpcServer);
    stubChannelWrite(rpcServer, new TestGatheringByteChannel(1, Integer.MAX_VALUE));
    BufferChain responseBuffer = new BufferChain(ByteBuffer.wrap(new byte[] { 1, 2 }));
    RpcResponse response = () -> responseBuffer;

    assertFalse(processResponse(connection, response));
    assertTrue(connection.lastSentTime > 0);
    assertTrue(processResponse(connection, response));
    assertEquals(-1L, connection.lastSentTime);
  }

  @Test
  public void testStalledPartialResponseKeepsLastSentTime() throws Exception {
    SimpleRpcServer rpcServer = mockRpcServer();
    responder = new SimpleRpcServerResponder(rpcServer);
    SimpleServerRpcConnection connection = newConnection(rpcServer);
    connection.lastSentTime = 123L;
    stubChannelWrite(rpcServer, new TestGatheringByteChannel(0));
    RpcResponse response = () -> new BufferChain(ByteBuffer.allocate(1));

    assertFalse(processResponse(connection, response));
    assertEquals(123L, connection.lastSentTime);
  }

  @Test
  public void testPurgeIgnoresConnectionWithEmptyResponseQueue() throws Exception {
    SimpleRpcServer rpcServer = mockRpcServer();
    responder = new SimpleRpcServerResponder(rpcServer);
    SimpleServerRpcConnection connection = newRegisteredConnection(rpcServer);
    connection.lastSentTime = 1L;

    purge(0L);

    assertTrue(connection.responseQueue.isEmpty());
    Mockito.verify(rpcServer, Mockito.never()).closeConnection(connection);
  }

  private boolean processResponse(SimpleServerRpcConnection connection, RpcResponse response)
      throws Exception {
    Method processResponse = SimpleRpcServerResponder.class.getDeclaredMethod("processResponse",
        SimpleServerRpcConnection.class, RpcResponse.class);
    processResponse.setAccessible(true);
    return (Boolean) processResponse.invoke(responder, connection, response);
  }

  private void purge(long lastPurgeTime) throws Exception {
    Method purge = SimpleRpcServerResponder.class.getDeclaredMethod("purge", long.class);
    purge.setAccessible(true);
    purge.invoke(responder, lastPurgeTime);
  }

  private static SimpleRpcServer mockRpcServer() {
    SimpleRpcServer rpcServer = Mockito.mock(SimpleRpcServer.class);
    Mockito.when(rpcServer.getConf()).thenReturn(new Configuration(false));
    return rpcServer;
  }

  private static SimpleServerRpcConnection newConnection(SimpleRpcServer rpcServer) {
    SocketChannel channel = Mockito.mock(SocketChannel.class);
    Socket socket = Mockito.mock(Socket.class);
    Mockito.when(channel.socket()).thenReturn(socket);
    Mockito.when(socket.getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
    Mockito.when(socket.getPort()).thenReturn(12345);
    return new SimpleServerRpcConnection(rpcServer, channel, 0);
  }

  private SimpleServerRpcConnection newRegisteredConnection(SimpleRpcServer rpcServer)
      throws Exception {
    registeredChannel = SocketChannel.open();
    registeredChannel.configureBlocking(false);
    SimpleServerRpcConnection connection =
        new SimpleServerRpcConnection(rpcServer, registeredChannel, 0);
    registeredChannel.register(writeSelector(), SelectionKey.OP_WRITE, connection);
    return connection;
  }

  private Selector writeSelector() throws Exception {
    Field writeSelectorField = SimpleRpcServerResponder.class.getDeclaredField("writeSelector");
    writeSelectorField.setAccessible(true);
    return (Selector) writeSelectorField.get(responder);
  }

  private static void stubChannelWrite(SimpleRpcServer rpcServer, GatheringByteChannel channel)
      throws IOException {
    Mockito.doAnswer(invocation -> {
      BufferChain response = invocation.getArgument(1);
      return response.write(channel);
    }).when(rpcServer).channelWrite(Mockito.any(), Mockito.any());
  }

  private static final class TestGatheringByteChannel implements GatheringByteChannel {

    private final int[] writeLimits;
    private int writeCount;

    private TestGatheringByteChannel(int... writeLimits) {
      this.writeLimits = writeLimits;
    }

    @Override
    public long write(ByteBuffer[] sources, int offset, int length) {
      int limit = writeLimits[Math.min(writeCount++, writeLimits.length - 1)];
      long written = 0;
      for (int i = offset; i < offset + length && written < limit; i++) {
        ByteBuffer source = sources[i];
        int bytes = (int) Math.min(source.remaining(), limit - written);
        source.position(source.position() + bytes);
        written += bytes;
      }
      return written;
    }

    @Override
    public long write(ByteBuffer[] sources) {
      return write(sources, 0, sources.length);
    }

    @Override
    public int write(ByteBuffer source) {
      return (int) write(new ByteBuffer[] { source }, 0, 1);
    }

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public void close() {
    }
  }
}
