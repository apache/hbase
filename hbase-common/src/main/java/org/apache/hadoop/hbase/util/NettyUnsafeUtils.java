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
package org.apache.hadoop.hbase.util;

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOption;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelOutboundBuffer;

/**
 * Wraps some usages of netty's unsafe API, for ease of maintainability.
 */
@InterfaceAudience.Private
public final class NettyUnsafeUtils {

  private NettyUnsafeUtils() {
  }

  /**
   * Directly closes the channel, setting SO_LINGER to 0 and skipping any handlers in the pipeline.
   * This is useful for cases where it's important to immediately close without any delay.
   * Otherwise, pipeline handlers and even general TCP flows can cause a normal close to take
   * upwards of a few second or more. This will likely cause the client side to see either a
   * "Connection reset by peer" or unexpected ConnectionClosedException.
   * <p>
   * <b>It's necessary to call this from within the channel's eventLoop!</b>
   */
  public static void closeImmediately(Channel channel) {
    assert channel.eventLoop().inEventLoop();
    channel.config().setOption(ChannelOption.SO_LINGER, 0);
    channel.unsafe().close(channel.voidPromise());
  }

  /**
   * Get total bytes pending write to socket
   */
  public static long getTotalPendingOutboundBytes(Channel channel) {
    ChannelOutboundBuffer outboundBuffer = channel.unsafe().outboundBuffer();
    // can be null when the channel is closing
    if (outboundBuffer == null) {
      return 0;
    }
    return outboundBuffer.totalPendingWriteBytes();
  }
}
