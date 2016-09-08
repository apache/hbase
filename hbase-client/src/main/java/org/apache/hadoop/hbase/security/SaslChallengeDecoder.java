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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Decode the sasl challenge sent by RpcServer.
 */
@InterfaceAudience.Private
public class SaslChallengeDecoder extends ByteToMessageDecoder {

  private static final int MAX_CHALLENGE_SIZE = 1024 * 1024; // 1M

  private ByteBuf tryDecodeChallenge(ByteBuf in, int offset, int readableBytes) throws IOException {
    if (readableBytes < 4) {
      return null;
    }
    int len = in.getInt(offset);
    if (len <= 0) {
      // fall back to simple
      in.readerIndex(offset + 4);
      return in.retainedSlice(offset, 4);
    }
    if (len > MAX_CHALLENGE_SIZE) {
      throw new IOException(
          "Sasl challenge too large(" + len + "), max allowed is " + MAX_CHALLENGE_SIZE);
    }
    int totalLen = 4 + len;
    if (readableBytes < totalLen) {
      return null;
    }
    in.readerIndex(offset + totalLen);
    return in.retainedSlice(offset, totalLen);
  }

  // will throw a RemoteException out if data is enough, so do not need to return anything.
  private void tryDecodeError(ByteBuf in, int offset, int readableBytes) throws IOException {
    if (readableBytes < 4) {
      return;
    }
    int classLen = in.getInt(offset);
    if (classLen <= 0) {
      throw new IOException("Invalid exception class name length " + classLen);
    }
    if (classLen > MAX_CHALLENGE_SIZE) {
      throw new IOException("Exception class name length too large(" + classLen
          + "), max allowed is " + MAX_CHALLENGE_SIZE);
    }
    if (readableBytes < 4 + classLen + 4) {
      return;
    }
    int msgLen = in.getInt(offset + 4 + classLen);
    if (msgLen <= 0) {
      throw new IOException("Invalid exception message length " + msgLen);
    }
    if (msgLen > MAX_CHALLENGE_SIZE) {
      throw new IOException("Exception message length too large(" + msgLen + "), max allowed is "
          + MAX_CHALLENGE_SIZE);
    }
    int totalLen = classLen + msgLen + 8;
    if (readableBytes < totalLen) {
      return;
    }
    String className = in.toString(offset + 4, classLen, HConstants.UTF8_CHARSET);
    String msg = in.toString(offset + classLen + 8, msgLen, HConstants.UTF8_CHARSET);
    in.readerIndex(offset + totalLen);
    throw new RemoteException(className, msg);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    int readableBytes = in.readableBytes();
    if (readableBytes < 4) {
      return;
    }
    int offset = in.readerIndex();
    int status = in.getInt(offset);
    if (status == SaslStatus.SUCCESS.state) {
      ByteBuf challenge = tryDecodeChallenge(in, offset + 4, readableBytes - 4);
      if (challenge != null) {
        out.add(challenge);
      }
    } else {
      tryDecodeError(in, offset + 4, readableBytes - 4);
    }
  }
}
