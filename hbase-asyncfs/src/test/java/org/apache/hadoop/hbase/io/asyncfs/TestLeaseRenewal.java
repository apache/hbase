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
package org.apache.hadoop.hbase.io.asyncfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DummyDFSOutputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockedConstruction;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoop;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Make sure lease renewal works. Since it is in a background thread, normal read/write test can not
 * verify it.
 * <p>
 * See HBASE-28955 for more details.
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestLeaseRenewal extends AsyncFSTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLeaseRenewal.class);

  private static DistributedFileSystem FS;
  private static EventLoopGroup EVENT_LOOP_GROUP;
  private static Class<? extends Channel> CHANNEL_CLASS;
  private static StreamSlowMonitor MONITOR;

  @BeforeClass
  public static void setUp() throws Exception {
    startMiniDFSCluster(3);
    FS = CLUSTER.getFileSystem();
    EVENT_LOOP_GROUP = new NioEventLoopGroup();
    CHANNEL_CLASS = NioSocketChannel.class;
    MONITOR = StreamSlowMonitor.create(UTIL.getConfiguration(), "testMonitor");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (EVENT_LOOP_GROUP != null) {
      EVENT_LOOP_GROUP.shutdownGracefully().get();
    }
    shutdownMiniDFSCluster();
  }

  private FanOutOneBlockAsyncDFSOutput create(String file)
    throws IllegalArgumentException, IOException {
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    return FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, new Path("/test_lease_renew"), true,
      false, (short) 3, FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS, MONITOR, true);
  }

  @Test
  public void testLeaseRenew() throws IOException {
    DFSClient client = FS.getClient();
    assertFalse(client.renewLease());

    FanOutOneBlockAsyncDFSOutput out = create("/test_lease_renew");
    assertTrue(client.renewLease());
    client.closeAllFilesBeingWritten(false);
    assertTrue(out.isClosed());

    assertFalse(client.renewLease());

    out = create("/test_lease_renew");
    assertTrue(client.renewLease());
    client.closeAllFilesBeingWritten(true);
    assertTrue(out.isClosed());
  }

  private Optional<Method> getUniqKeyMethod() {
    try {
      return Optional.of(DFSOutputStream.class.getMethod("getUniqKey"));
    } catch (NoSuchMethodException e) {
      // should be hadoop 3.3 or below
      return Optional.empty();
    }
  }

  @Test
  public void testEnsureMethodsCalledWhenLeaseRenewal() throws Exception {
    try (MockedConstruction<DummyDFSOutputStream> mocked =
      mockConstruction(DummyDFSOutputStream.class)) {
      try (FanOutOneBlockAsyncDFSOutput out = create("/methods_for_lease_renewal")) {
        DummyDFSOutputStream dummy = mocked.constructed().get(0);
        assertTrue(FS.getClient().renewLease());
        Optional<Method> getUniqKeyMethod = getUniqKeyMethod();
        if (getUniqKeyMethod.isPresent()) {
          getUniqKeyMethod.get().invoke(verify(dummy));
          Method getNamespaceMethod = DFSOutputStream.class.getMethod("getNamespace");
          getNamespaceMethod.invoke(verify(dummy));
        }
        verifyNoMoreInteractions(dummy);
      }
    }
  }

  private void verifyGetUniqKey(DummyDFSOutputStream dummy) throws Exception {
    Optional<Method> getUniqKeyMethod = getUniqKeyMethod();
    if (getUniqKeyMethod.isPresent()) {
      getUniqKeyMethod.get().invoke(verify(dummy));
    }
  }

  @Test
  public void testEnsureMethodsCalledWhenClosing() throws Exception {
    try (MockedConstruction<DummyDFSOutputStream> mocked =
      mockConstruction(DummyDFSOutputStream.class)) {
      try (FanOutOneBlockAsyncDFSOutput out = create("/methods_for_closing")) {
        DummyDFSOutputStream dummy = mocked.constructed().get(0);
        verifyGetUniqKey(dummy);
        FS.getClient().closeAllFilesBeingWritten(false);
        verify(dummy).close();

        verifyNoMoreInteractions(dummy);
      }
    }
  }

  @Test
  public void testEnsureMethodsCalledWhenAborting() throws Exception {
    try (MockedConstruction<DummyDFSOutputStream> mocked =
      mockConstruction(DummyDFSOutputStream.class)) {
      try (FanOutOneBlockAsyncDFSOutput out = create("/methods_for_aborting")) {
        DummyDFSOutputStream dummy = mocked.constructed().get(0);
        verifyGetUniqKey(dummy);
        FS.getClient().closeAllFilesBeingWritten(true);
        verify(dummy).abort();
        verifyNoMoreInteractions(dummy);
      }
    }
  }
}
