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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.RpcServer.CallCleanup;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingService;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.htrace.TraceInfo;

/**
 * RpcConnection implementation for netty rpc server.
 */
@InterfaceAudience.Private
class NettyServerRpcConnection extends ServerRpcConnection {

  final Channel channel;

  NettyServerRpcConnection(NettyRpcServer rpcServer, Channel channel) {
    super(rpcServer);
    this.channel = channel;
    InetSocketAddress inetSocketAddress = ((InetSocketAddress) channel.remoteAddress());
    this.addr = inetSocketAddress.getAddress();
    if (addr == null) {
      this.hostAddress = "*Unknown*";
    } else {
      this.hostAddress = inetSocketAddress.getAddress().getHostAddress();
    }
    this.remotePort = inetSocketAddress.getPort();
    this.saslCall = new NettyServerCall(SASL_CALLID, null, null, null, null, null, this, 0, null,
        null, System.currentTimeMillis(), 0, rpcServer.reservoir, rpcServer.cellBlockBuilder, null);
    this.setConnectionHeaderResponseCall = new NettyServerCall(CONNECTION_HEADER_RESPONSE_CALLID,
        null, null, null, null, null, this, 0, null, null, System.currentTimeMillis(), 0,
        rpcServer.reservoir, rpcServer.cellBlockBuilder, null);
    this.authFailedCall = new NettyServerCall(AUTHORIZATION_FAILED_CALLID, null, null, null, null,
        null, this, 0, null, null, System.currentTimeMillis(), 0, rpcServer.reservoir,
        rpcServer.cellBlockBuilder, null);
  }

  void readPreamble(ByteBuf buffer) throws IOException {
    byte[] rpcHead = { buffer.readByte(), buffer.readByte(), buffer.readByte(), buffer.readByte() };
    if (!Arrays.equals(HConstants.RPC_HEADER, rpcHead)) {
      doBadPreambleHandling("Expected HEADER=" + Bytes.toStringBinary(HConstants.RPC_HEADER) +
          " but received HEADER=" + Bytes.toStringBinary(rpcHead) + " from " + toString());
      return;
    }
    // Now read the next two bytes, the version and the auth to use.
    int version = buffer.readByte();
    byte authbyte = buffer.readByte();
    this.authMethod = AuthMethod.valueOf(authbyte);
    if (version != NettyRpcServer.CURRENT_VERSION) {
      String msg = getFatalConnectionString(version, authbyte);
      doBadPreambleHandling(msg, new WrongVersionException(msg));
      return;
    }
    if (authMethod == null) {
      String msg = getFatalConnectionString(version, authbyte);
      doBadPreambleHandling(msg, new BadAuthException(msg));
      return;
    }
    if (this.rpcServer.isSecurityEnabled && authMethod == AuthMethod.SIMPLE) {
      if (this.rpcServer.allowFallbackToSimpleAuth) {
        this.rpcServer.metrics.authenticationFallback();
        authenticatedWithFallback = true;
      } else {
        AccessDeniedException ae = new AccessDeniedException("Authentication is required");
        this.rpcServer.setupResponse(authFailedResponse, authFailedCall, ae, ae.getMessage());
        ((NettyServerCall) authFailedCall).sendResponseIfReady(ChannelFutureListener.CLOSE);
        return;
      }
    }
    if (!this.rpcServer.isSecurityEnabled && authMethod != AuthMethod.SIMPLE) {
      doRawSaslReply(SaslStatus.SUCCESS, new IntWritable(SaslUtil.SWITCH_TO_SIMPLE_AUTH), null,
        null);
      authMethod = AuthMethod.SIMPLE;
      // client has already sent the initial Sasl message and we
      // should ignore it. Both client and server should fall back
      // to simple auth from now on.
      skipInitialSaslHandshake = true;
    }
    if (authMethod != AuthMethod.SIMPLE) {
      useSasl = true;
    }
    connectionPreambleRead = true;
  }

  private void doBadPreambleHandling(final String msg) throws IOException {
    doBadPreambleHandling(msg, new FatalConnectionException(msg));
  }

  private void doBadPreambleHandling(final String msg, final Exception e) throws IOException {
    NettyRpcServer.LOG.warn(msg);
    NettyServerCall fakeCall = new NettyServerCall(-1, null, null, null, null, null, this, -1, null,
        null, System.currentTimeMillis(), 0, this.rpcServer.reservoir,
        this.rpcServer.cellBlockBuilder, null);
    this.rpcServer.setupResponse(null, fakeCall, e, msg);
    // closes out the connection.
    fakeCall.sendResponseIfReady(ChannelFutureListener.CLOSE);
  }

  void process(final ByteBuf buf) throws IOException, InterruptedException {
    if (connectionHeaderRead) {
      this.callCleanup = new RpcServer.CallCleanup() {
        @Override
        public void run() {
          buf.release();
        }
      };
      process(new SingleByteBuff(buf.nioBuffer()));
    } else {
      byte[] data = new byte[buf.readableBytes()];
      buf.readBytes(data, 0, data.length);
      ByteBuffer connectionHeader = ByteBuffer.wrap(data);
      buf.release();
      process(connectionHeader);
    }
  }

  void process(ByteBuffer buf) throws IOException, InterruptedException {
    process(new SingleByteBuff(buf));
  }

  void process(ByteBuff buf) throws IOException, InterruptedException {
    try {
      if (skipInitialSaslHandshake) {
        skipInitialSaslHandshake = false;
        if (callCleanup != null) {
          callCleanup.run();
        }
        return;
      }

      if (useSasl) {
        saslReadAndProcess(buf);
      } else {
        processOneRpc(buf);
      }
    } catch (Exception e) {
      if (callCleanup != null) {
        callCleanup.run();
      }
      throw e;
    } finally {
      this.callCleanup = null;
    }
  }

  @Override
  public synchronized void close() {
    disposeSasl();
    channel.close();
    callCleanup = null;
  }

  @Override
  public boolean isConnectionOpen() {
    return channel.isOpen();
  }

  @Override
  public NettyServerCall createCall(int id, final BlockingService service,
      final MethodDescriptor md, RequestHeader header, Message param, CellScanner cellScanner,
      long size, TraceInfo tinfo, final InetAddress remoteAddress, int timeout,
      CallCleanup reqCleanup) {
    return new NettyServerCall(id, service, md, header, param, cellScanner, this, size, tinfo,
        remoteAddress, System.currentTimeMillis(), timeout, this.rpcServer.reservoir,
        this.rpcServer.cellBlockBuilder, reqCleanup);
  }
}