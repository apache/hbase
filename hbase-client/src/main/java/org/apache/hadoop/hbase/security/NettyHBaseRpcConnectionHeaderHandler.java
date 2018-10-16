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

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hbase.thirdparty.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Promise;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

/**
 * Implement logic to deal with the rpc connection header.
 * @since 2.0.0
 */
@InterfaceAudience.Private
public class NettyHBaseRpcConnectionHeaderHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private final Promise<Boolean> saslPromise;

  private final Configuration conf;

  private final ByteBuf connectionHeaderWithLength;

  public NettyHBaseRpcConnectionHeaderHandler(Promise<Boolean> saslPromise, Configuration conf,
                                              ByteBuf connectionHeaderWithLength) {
    this.saslPromise = saslPromise;
    this.conf = conf;
    this.connectionHeaderWithLength = connectionHeaderWithLength;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    // read the ConnectionHeaderResponse from server
    int len = msg.readInt();
    byte[] buff = new byte[len];
    msg.readBytes(buff);

    RPCProtos.ConnectionHeaderResponse connectionHeaderResponse =
        RPCProtos.ConnectionHeaderResponse.parseFrom(buff);

    // Get the CryptoCipherMeta, update the HBaseSaslRpcClient for Crypto Cipher
    if (connectionHeaderResponse.hasCryptoCipherMeta()) {
      CryptoAES cryptoAES = EncryptionUtil.createCryptoAES(
          connectionHeaderResponse.getCryptoCipherMeta(), conf);
      // replace the Sasl handler with Crypto AES handler
      setupCryptoAESHandler(ctx.pipeline(), cryptoAES);
    }

    saslPromise.setSuccess(true);
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) {
    try {
      // send the connection header to server first
      ctx.writeAndFlush(connectionHeaderWithLength.retainedDuplicate());
    } catch (Exception e) {
      // the exception thrown by handlerAdded will not be passed to the exceptionCaught below
      // because netty will remove a handler if handlerAdded throws an exception.
      exceptionCaught(ctx, e);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    saslPromise.tryFailure(cause);
  }

  /**
   * Remove handlers for sasl encryption and add handlers for Crypto AES encryption
   */
  private void setupCryptoAESHandler(ChannelPipeline p, CryptoAES cryptoAES) {
    p.remove(SaslWrapHandler.class);
    p.remove(SaslUnwrapHandler.class);
    String lengthDecoder = p.context(LengthFieldBasedFrameDecoder.class).name();
    p.addAfter(lengthDecoder, null, new CryptoAESUnwrapHandler(cryptoAES));
    p.addAfter(lengthDecoder, null, new CryptoAESWrapHandler(cryptoAES));
  }
}
