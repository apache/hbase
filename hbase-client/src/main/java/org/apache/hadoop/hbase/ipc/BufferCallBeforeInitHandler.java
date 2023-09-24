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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.channel.ChannelDuplexHandler;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPromise;

/**
 * We will expose the connection to upper layer before initialized, so we need to buffer the calls
 * passed in and write them out once the connection is established.
 */
@InterfaceAudience.Private
class BufferCallBeforeInitHandler extends ChannelDuplexHandler {

  private static final Logger LOG = LoggerFactory.getLogger(BufferCallBeforeInitHandler.class);

  static final String NAME = "BufferCall";

  private enum BufferCallAction {
    FLUSH,
    FAIL
  }

  public static final class BufferCallEvent {

    public final BufferCallAction action;

    public final IOException error;

    private BufferCallEvent(BufferCallBeforeInitHandler.BufferCallAction action,
      IOException error) {
      this.action = action;
      this.error = error;
    }

    public static BufferCallBeforeInitHandler.BufferCallEvent success() {
      return SUCCESS_EVENT;
    }

    public static BufferCallBeforeInitHandler.BufferCallEvent fail(IOException error) {
      return new BufferCallEvent(BufferCallAction.FAIL, error);
    }
  }

  private static final BufferCallEvent SUCCESS_EVENT =
    new BufferCallEvent(BufferCallAction.FLUSH, null);

  private final Map<Integer, Call> id2Call = new HashMap<>();

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    if (msg instanceof Call) {
      Call call = (Call) msg;
      id2Call.put(call.id, call);
      // The call is already in track so here we set the write operation as success.
      // We will fail the call directly if we can not write it out.
      promise.trySuccess();
    } else {
      ctx.write(msg, promise);
    }
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    // do not flush anything out
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof BufferCallEvent) {
      BufferCallEvent bcEvt = (BufferCallBeforeInitHandler.BufferCallEvent) evt;
      switch (bcEvt.action) {
        case FLUSH:
          for (Call call : id2Call.values()) {
            ctx.write(call);
          }
          ctx.flush();
          ctx.pipeline().remove(this);
          break;
        case FAIL:
          for (Call call : id2Call.values()) {
            call.setException(bcEvt.error);
          }
          // here we do not remove us from the pipeline, for receiving possible exceptions and log
          // it, especially the ssl exceptions, to prevent it reaching the tail of the pipeline and
          // generate a confusing netty WARN
          break;
      }
    } else if (evt instanceof CallEvent) {
      // just remove the call for now until we add other call event other than timeout and cancel.
      id2Call.remove(((CallEvent) evt).call.id);
    } else {
      ctx.fireUserEventTriggered(evt);
    }
  }

  private boolean isSslError(Throwable cause) {
    Throwable error = cause;
    do {
      if (error instanceof SSLException) {
        return true;
      }
      error = error.getCause();
    } while (error != null);
    return false;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (isSslError(cause)) {
      // this should have been logged in other places, see HBASE-27782 for more details.
      // here we just log it with debug and tell users that this is not a critical problem,
      // otherwise if we just pass it through the pipeline, it will lead to a confusing
      // "An exceptionCaught() event was fired, and it reached at the tail of the pipeline"
      LOG.debug(
        "got ssl exception, which should have already been proceeded, log it here to"
          + " prevent it being passed to netty's TailContext and trigger a confusing WARN message",
        cause);
    } else {
      ctx.fireExceptionCaught(cause);
    }
  }
}
