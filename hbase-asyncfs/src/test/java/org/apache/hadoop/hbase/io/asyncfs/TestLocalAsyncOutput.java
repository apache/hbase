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

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestLocalAsyncOutput {

  private static EventLoopGroup GROUP = new NioEventLoopGroup();

  private static Class<? extends Channel> CHANNEL_CLASS = NioSocketChannel.class;

  private static final HBaseCommonTestingUtility TEST_UTIL = new HBaseCommonTestingUtility();

  private static StreamSlowMonitor MONITOR;

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.cleanupTestDir();
    GROUP.shutdownGracefully().get();
    MONITOR = StreamSlowMonitor.create(TEST_UTIL.getConfiguration(), "testMonitor");
  }

  @Test
  public void test() throws IOException, InterruptedException, ExecutionException,
    CommonFSUtils.StreamLacksCapabilityException {
    Path f = new Path(TEST_UTIL.getDataTestDir(), "test");
    FileSystem fs = FileSystem.getLocal(TEST_UTIL.getConfiguration());
    AsyncFSOutput out = AsyncFSOutputHelper.createOutput(fs, f, false, true,
      fs.getDefaultReplication(f), fs.getDefaultBlockSize(f), GROUP, CHANNEL_CLASS, MONITOR, true);
    TestFanOutOneBlockAsyncDFSOutput.writeAndVerify(fs, f, out);
  }
}
