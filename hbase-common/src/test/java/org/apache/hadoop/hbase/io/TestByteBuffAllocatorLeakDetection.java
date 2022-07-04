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
package org.apache.hadoop.hbase.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.io.netty.util.ResourceLeakDetector;
import org.apache.hbase.thirdparty.io.netty.util.internal.logging.InternalLogLevel;
import org.apache.hbase.thirdparty.io.netty.util.internal.logging.InternalLogger;
import org.apache.hbase.thirdparty.io.netty.util.internal.logging.InternalLoggerFactory;
import org.apache.hbase.thirdparty.io.netty.util.internal.logging.Slf4JLoggerFactory;

@Category({ RPCTests.class, SmallTests.class })
public class TestByteBuffAllocatorLeakDetection {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestByteBuffAllocatorLeakDetection.class);

  @SuppressWarnings("unused")
  @Test
  public void testLeakDetection() throws InterruptedException {
    InternalLoggerFactory original = InternalLoggerFactory.getDefaultFactory();
    AtomicInteger leaksDetected = new AtomicInteger();
    InternalLoggerFactory.setDefaultFactory(new MockedLoggerFactory(leaksDetected));

    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    assertTrue(ResourceLeakDetector.isEnabled());

    try {
      int maxBuffersInPool = 10;
      int bufSize = 1024;
      int minSize = bufSize / 8;
      ByteBuffAllocator alloc = new ByteBuffAllocator(true, maxBuffersInPool, bufSize, minSize);

      // tracking leaks happens on creation of a RefCnt, through a call to detector.track().
      // If a leak occurs, but detector.track() is never called again, the leak will not be
      // realized. Further, causing a leak requires a GC event. So below we do some allocations,
      // cause some GC's, do more allocations, and then expect a leak to show up.

      // first allocate on-heap. we expect to not see a leak from this, because we
      // dont count on-heap references
      alloc.allocate(minSize - 1);
      System.gc();
      Thread.sleep(1000);

      // cause an allocation to trigger a leak detection, if there were one.
      // keep a reference so we don't trigger a leak right away from this.
      ByteBuff reference = alloc.allocate(minSize * 2);
      assertEquals(0, leaksDetected.get());

      // allocate, but don't keep a reference. this should cause a leak
      alloc.allocate(minSize * 2);
      System.gc();
      Thread.sleep(1000);

      // allocate again, this should cause the above leak to be detected
      alloc.allocate(minSize * 2);
      assertEquals(1, leaksDetected.get());
    } finally {
      InternalLoggerFactory.setDefaultFactory(original);
    }
  }

  private static class MockedLoggerFactory extends Slf4JLoggerFactory {

    private AtomicInteger leaksDetected;

    public MockedLoggerFactory(AtomicInteger leaksDetected) {
      this.leaksDetected = leaksDetected;
    }

    @Override
    public InternalLogger newInstance(String name) {
      InternalLogger delegate = super.newInstance(name);
      return new MockedLogger(leaksDetected, delegate);
    }
  }

  private static class MockedLogger implements InternalLogger {

    private AtomicInteger leaksDetected;
    private InternalLogger delegate;

    public MockedLogger(AtomicInteger leaksDetected, InternalLogger delegate) {
      this.leaksDetected = leaksDetected;
      this.delegate = delegate;
    }

    private void maybeCountLeak(String msgOrFormat) {
      if (msgOrFormat.startsWith("LEAK")) {
        leaksDetected.incrementAndGet();
      }
    }

    @Override
    public void error(String msg) {
      maybeCountLeak(msg);
      delegate.error(msg);
    }

    @Override
    public void error(String format, Object arg) {
      maybeCountLeak(format);
      delegate.error(format, arg);
    }

    @Override
    public void error(String format, Object argA, Object argB) {
      maybeCountLeak(format);
      delegate.error(format, argA, argB);
    }

    @Override
    public void error(String format, Object... arguments) {
      maybeCountLeak(format);
      delegate.error(format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
      maybeCountLeak(msg);
      delegate.error(msg, t);
    }

    @Override
    public void error(Throwable t) {
      delegate.error(t);
    }

    @Override
    public String name() {
      return delegate.name();
    }

    @Override
    public boolean isTraceEnabled() {
      return delegate.isTraceEnabled();
    }

    @Override
    public void trace(String msg) {
      delegate.trace(msg);
    }

    @Override
    public void trace(String format, Object arg) {
      delegate.trace(format, arg);
    }

    @Override
    public void trace(String format, Object argA, Object argB) {
      delegate.trace(format, argA, argB);
    }

    @Override
    public void trace(String format, Object... arguments) {
      delegate.trace(format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
      delegate.trace(msg, t);
    }

    @Override
    public void trace(Throwable t) {
      delegate.trace(t);
    }

    @Override
    public boolean isDebugEnabled() {
      return delegate.isDebugEnabled();
    }

    @Override
    public void debug(String msg) {
      delegate.debug(msg);
    }

    @Override
    public void debug(String format, Object arg) {
      delegate.debug(format, arg);
    }

    @Override
    public void debug(String format, Object argA, Object argB) {
      delegate.debug(format, argA, argB);
    }

    @Override
    public void debug(String format, Object... arguments) {
      delegate.debug(format, arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
      delegate.debug(msg, t);
    }

    @Override
    public void debug(Throwable t) {
      delegate.debug(t);
    }

    @Override
    public boolean isInfoEnabled() {
      return delegate.isInfoEnabled();
    }

    @Override
    public void info(String msg) {
      delegate.info(msg);
    }

    @Override
    public void info(String format, Object arg) {
      delegate.info(format, arg);
    }

    @Override
    public void info(String format, Object argA, Object argB) {
      delegate.info(format, argA, argB);
    }

    @Override
    public void info(String format, Object... arguments) {
      delegate.info(format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
      delegate.info(msg, t);
    }

    @Override
    public void info(Throwable t) {
      delegate.info(t);
    }

    @Override
    public boolean isWarnEnabled() {
      return delegate.isWarnEnabled();
    }

    @Override
    public void warn(String msg) {
      delegate.warn(msg);
    }

    @Override
    public void warn(String format, Object arg) {
      delegate.warn(format, arg);
    }

    @Override
    public void warn(String format, Object... arguments) {
      delegate.warn(format, arguments);
    }

    @Override
    public void warn(String format, Object argA, Object argB) {
      delegate.warn(format, argA, argB);
    }

    @Override
    public void warn(String msg, Throwable t) {
      delegate.warn(msg, t);
    }

    @Override
    public void warn(Throwable t) {
      delegate.warn(t);
    }

    @Override
    public boolean isErrorEnabled() {
      return delegate.isErrorEnabled();
    }

    @Override
    public boolean isEnabled(InternalLogLevel level) {
      return delegate.isEnabled(level);
    }

    @Override
    public void log(InternalLogLevel level, String msg) {
      delegate.log(level, msg);
    }

    @Override
    public void log(InternalLogLevel level, String format, Object arg) {
      delegate.log(level, format, arg);
    }

    @Override
    public void log(InternalLogLevel level, String format, Object argA, Object argB) {
      delegate.log(level, format, argA, argB);
    }

    @Override
    public void log(InternalLogLevel level, String format, Object... arguments) {
      delegate.log(level, format, arguments);
    }

    @Override
    public void log(InternalLogLevel level, String msg, Throwable t) {
      delegate.log(level, msg, t);
    }

    @Override
    public void log(InternalLogLevel level, Throwable t) {
      delegate.log(level, t);
    }
  }
}
