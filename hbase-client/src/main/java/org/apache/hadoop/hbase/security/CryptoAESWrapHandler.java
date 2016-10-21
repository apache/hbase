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

package org.apache.hadoop.hbase.security;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.CoalescingBufferQueue;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;

import java.io.IOException;

/**
 * wrap messages with Crypto AES.
 */
@InterfaceAudience.Private
public class CryptoAESWrapHandler extends ChannelOutboundHandlerAdapter {

  private final CryptoAES cryptoAES;

  private CoalescingBufferQueue queue;

  public CryptoAESWrapHandler(CryptoAES cryptoAES) {
    this.cryptoAES = cryptoAES;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    queue = new CoalescingBufferQueue(ctx.channel());
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (msg instanceof ByteBuf) {
      queue.add((ByteBuf) msg, promise);
    } else {
      ctx.write(msg, promise);
    }
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    if (queue.isEmpty()) {
      return;
    }
    ByteBuf buf = null;
    try {
      ChannelPromise promise = ctx.newPromise();
      int readableBytes = queue.readableBytes();
      buf = queue.remove(readableBytes, promise);
      byte[] bytes = new byte[readableBytes];
      buf.readBytes(bytes);
      byte[] wrapperBytes = cryptoAES.wrap(bytes, 0, bytes.length);
      ChannelPromise lenPromise = ctx.newPromise();
      ctx.write(ctx.alloc().buffer(4).writeInt(wrapperBytes.length), lenPromise);
      ChannelPromise contentPromise = ctx.newPromise();
      ctx.write(Unpooled.wrappedBuffer(wrapperBytes), contentPromise);
      PromiseCombiner combiner = new PromiseCombiner();
      combiner.addAll(lenPromise, contentPromise);
      combiner.finish(promise);
      ctx.flush();
    } finally {
      if (buf != null) {
        ReferenceCountUtil.safeRelease(buf);
      }
    }
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    if (!queue.isEmpty()) {
      queue.releaseAndFailAll(new IOException("Connection closed"));
    }
    ctx.close(promise);
  }
}
