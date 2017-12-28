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

import org.apache.hbase.thirdparty.io.netty.buffer.Unpooled;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPromise;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Encoder for {@link RpcResponse}.
 * @since 2.0.0
 */
@InterfaceAudience.Private
class NettyRpcServerResponseEncoder extends ChannelOutboundHandlerAdapter {

  private final MetricsHBaseServer metrics;

  NettyRpcServerResponseEncoder(MetricsHBaseServer metrics) {
    this.metrics = metrics;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (msg instanceof RpcResponse) {
      RpcResponse resp = (RpcResponse) msg;
      BufferChain buf = resp.getResponse();
      ctx.write(Unpooled.wrappedBuffer(buf.getBuffers()), promise).addListener(f -> {
        resp.done();
        if (f.isSuccess()) {
          metrics.sentBytes(buf.size());
        }
      });
    } else {
      ctx.write(msg, promise);
    }
  }
}
