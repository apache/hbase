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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.channel.ChannelOutboundInvoker;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Future;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.GenericFutureListener;

/**
 * Helper class for processing netty futures.
 */
@InterfaceAudience.Private
public final class NettyFutureUtils {

  private static final Logger LOG = LoggerFactory.getLogger(NettyFutureUtils.class);

  private NettyFutureUtils() {
  }

  /**
   * This is method is used when you just want to add a listener to the given netty future. Ignoring
   * the return value of a Future is considered as a bad practice as it may suppress exceptions
   * thrown from the code that completes the future, and this method will catch all the exception
   * thrown from the {@code listener} to catch possible code bugs.
   * <p/>
   * And the error phone check will always report FutureReturnValueIgnored because every method in
   * the {@link Future} class will return a new {@link Future}, so you always have one future that
   * has not been checked. So we introduce this method and add a suppress warnings annotation here.
   */
  @SuppressWarnings({ "FutureReturnValueIgnored", "rawtypes", "unchecked" })
  public static <V> void addListener(Future<V> future,
    GenericFutureListener<? extends Future<? super V>> listener) {
    future.addListener(f -> {
      try {
        // the ? operator in template makes it really hard to pass compile, so here we just cast the
        // listener to raw type.
        ((GenericFutureListener) listener).operationComplete(f);
      } catch (Throwable t) {
        LOG.error("Unexpected error caught when processing netty", t);
      }
    });
  }

  private static void loggingWhenError(Future<?> future) {
    if (!future.isSuccess()) {
      LOG.warn("IO operation failed", future.cause());
    }
  }

  /**
   * Log the error if the future indicates any failure.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public static void consume(Future<?> future) {
    future.addListener(NettyFutureUtils::loggingWhenError);
  }

  /**
   * Close the channel and eat the returned future by logging the error when the future is completed
   * with error.
   */
  public static void safeClose(ChannelOutboundInvoker channel) {
    consume(channel.close());
  }

  /**
   * Call write on the channel and eat the returned future by logging the error when the future is
   * completed with error.
   */
  public static void safeWrite(ChannelOutboundInvoker channel, Object msg) {
    consume(channel.write(msg));
  }

  /**
   * Call writeAndFlush on the channel and eat the returned future by logging the error when the
   * future is completed with error.
   */
  public static void safeWriteAndFlush(ChannelOutboundInvoker channel, Object msg) {
    consume(channel.writeAndFlush(msg));
  }
}
