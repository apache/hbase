/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.ipc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Promise;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.SaslUtil;

/**
 * Implements a Netty RPC client handler for the preamble response from the RPC server
 */
@InterfaceAudience.Private
public class NettyRpcNegotiateHandler extends ChannelDuplexHandler {
  private static final Log LOG = LogFactory.getLog(NettyRpcNegotiateHandler.class);
  private final RpcConnection conn;
  private final Promise<Boolean> promise;
  private final boolean fallbackAllowed;

  public NettyRpcNegotiateHandler(RpcConnection conn, Promise<Boolean> promise,
                                  boolean fallbackAllowed) {
    this.conn = conn;
    this.promise = promise;
    this.fallbackAllowed = fallbackAllowed;
  }

  private void attemptToFallback() {
    if (fallbackAllowed) {
      LOG.info("Server asked us to fall back to SIMPLE auth. Falling back...");
      conn.useSasl = false;
    } else {
      LOG.error("Server asked us to fall back to SIMPLE auth, " +
          "but we are not configured for that behavior!");
      handleFailure(new FallbackDisallowedException());
    }
  }

  private void handleFailure(Exception e) {
    promise.tryFailure(e);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf buf = (ByteBuf) msg;
    int status;
    try {
      status = buf.readInt();
      if (status == 0) {
        if (buf.readInt() == SaslUtil.SWITCH_TO_SIMPLE_AUTH) {
          attemptToFallback();
        }
        promise.trySuccess(true);
      }
      handleFailure(new IOException("Error while establishing connection to server"));
    } catch (Exception e) {
      handleFailure(e);
    } finally {
      buf.release();
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    conn.shutdown();
  }
}
