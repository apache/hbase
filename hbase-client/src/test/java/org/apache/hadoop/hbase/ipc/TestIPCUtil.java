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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.io.netty.channel.DefaultEventLoop;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoop;

@Category({ ClientTests.class, SmallTests.class })
public class TestIPCUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestIPCUtil.class);

  private static Throwable create(Class<? extends Throwable> clazz) throws InstantiationException,
    IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    try {
      Constructor<? extends Throwable> c = clazz.getDeclaredConstructor();
      c.setAccessible(true);
      return c.newInstance();
    } catch (NoSuchMethodException e) {
      // fall through
    }

    try {
      Constructor<? extends Throwable> c = clazz.getDeclaredConstructor(String.class);
      c.setAccessible(true);
      return c.newInstance("error");
    } catch (NoSuchMethodException e) {
      // fall through
    }

    try {
      Constructor<? extends Throwable> c = clazz.getDeclaredConstructor(Throwable.class);
      c.setAccessible(true);
      return c.newInstance(new Exception("error"));
    } catch (NoSuchMethodException e) {
      // fall through
    }

    try {
      Constructor<? extends Throwable> c =
        clazz.getDeclaredConstructor(String.class, Throwable.class);
      c.setAccessible(true);
      return c.newInstance("error", new Exception("error"));
    } catch (NoSuchMethodException e) {
      // fall through
    }

    Constructor<? extends Throwable> c =
      clazz.getDeclaredConstructor(Throwable.class, Throwable.class);
    c.setAccessible(true);
    return c.newInstance(new Exception("error"), "error");
  }

  /**
   * See HBASE-21862, it is important to keep original exception type for connection exceptions.
   */
  @Test
  public void testWrapConnectionException() throws Exception {
    List<Throwable> exceptions = new ArrayList<>();
    for (Class<? extends Throwable> clazz : ClientExceptionsUtil.getConnectionExceptionTypes()) {
      exceptions.add(create(clazz));
    }
    InetSocketAddress addr = InetSocketAddress.createUnresolved("127.0.0.1", 12345);
    for (Throwable exception : exceptions) {
      if (exception instanceof TimeoutException) {
        assertThat(IPCUtil.wrapException(addr, null, exception), instanceOf(TimeoutIOException.class));
      } else {
        IOException ioe = IPCUtil.wrapException(addr, RegionInfoBuilder.FIRST_META_REGIONINFO,
          exception);
        // Assert that the exception contains the Region name if supplied. HBASE-25735.
        // Not all exceptions get the region stuffed into it.
        if (ioe.getMessage() != null) {
          assertTrue(ioe.getMessage().
            contains(RegionInfoBuilder.FIRST_META_REGIONINFO.getRegionNameAsString()));
        }
        assertThat(ioe, instanceOf(exception.getClass()));
      }
    }
  }

  @Test
  public void testExecute() throws IOException {
    EventLoop eventLoop = new DefaultEventLoop();
    MutableInt executed = new MutableInt(0);
    MutableInt numStackTraceElements = new MutableInt(0);
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      IPCUtil.execute(eventLoop, new Runnable() {

        @Override
        public void run() {
          int numElements = new Exception().getStackTrace().length;
          int depth = executed.getAndIncrement();
          if (depth <= IPCUtil.MAX_DEPTH) {
            if (numElements <= numStackTraceElements.intValue()) {
              future.completeExceptionally(
                new AssertionError("should call run directly but stack trace decreased from " +
                  numStackTraceElements.intValue() + " to " + numElements));
              return;
            }
            numStackTraceElements.setValue(numElements);
            IPCUtil.execute(eventLoop, this);
          } else {
            if (numElements >= numStackTraceElements.intValue()) {
              future.completeExceptionally(
                new AssertionError("should call eventLoop.execute to prevent stack overflow but" +
                  " stack trace increased from " + numStackTraceElements.intValue() + " to " +
                  numElements));
            } else {
              future.complete(null);
            }
          }
        }
      });
      FutureUtils.get(future);
    } finally {
      eventLoop.shutdownGracefully();
    }
  }
}
