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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestAsyncWALReplay extends AbstractTestWALReplay {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncWALReplay.class);

  private static EventLoopGroup GROUP;

  private static Class<? extends Channel> CHANNEL_CLASS;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    GROUP = new NioEventLoopGroup(1,
      new ThreadFactoryBuilder().setNameFormat("TestAsyncWALReplay-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
    CHANNEL_CLASS = NioSocketChannel.class;
    Configuration conf = AbstractTestWALReplay.TEST_UTIL.getConfiguration();
    conf.set(WALFactory.WAL_PROVIDER, "asyncfs");
    AbstractTestWALReplay.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractTestWALReplay.tearDownAfterClass();
    GROUP.shutdownGracefully();
  }

  @Override
  protected WAL createWAL(Configuration c, Path hbaseRootDir, String logName) throws IOException {
    AsyncFSWAL asyncFSWAL = new AsyncFSWAL(FileSystem.get(c), hbaseRootDir, logName,
      HConstants.HREGION_OLDLOGDIR_NAME, c, null, true, null, null, GROUP, CHANNEL_CLASS);
    asyncFSWAL.init();
    return asyncFSWAL;
  }
}
