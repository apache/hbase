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
import java.io.InterruptedIOException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.wal.WALProvider.AsyncWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestAsyncProtobufLog extends AbstractTestProtobufLog<WALProvider.AsyncWriter> {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncProtobufLog.class);

  private static EventLoopGroup EVENT_LOOP_GROUP;

  private static Class<? extends Channel> CHANNEL_CLASS;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    EVENT_LOOP_GROUP = new NioEventLoopGroup();
    CHANNEL_CLASS = NioSocketChannel.class;
    AbstractTestProtobufLog.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractTestProtobufLog.tearDownAfterClass();
    EVENT_LOOP_GROUP.shutdownGracefully().syncUninterruptibly();
  }

  @Override
  protected AsyncWriter createWriter(Path path) throws IOException {
    return AsyncFSWALProvider.createAsyncWriter(TEST_UTIL.getConfiguration(), fs, path, false,
      EVENT_LOOP_GROUP.next(), CHANNEL_CLASS);
  }

  @Override
  protected void append(AsyncWriter writer, Entry entry) throws IOException {
    writer.append(entry);
  }

  @Override
  protected void sync(AsyncWriter writer) throws IOException {
    try {
      writer.sync(false).get();
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause());
      throw new IOException(e.getCause());
    }
  }
}
