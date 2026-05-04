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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestAsyncProtobufLog extends AbstractTestProtobufLog {

  private static EventLoopGroup EVENT_LOOP_GROUP;

  private static Class<? extends Channel> CHANNEL_CLASS;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    EVENT_LOOP_GROUP = new NioEventLoopGroup();
    CHANNEL_CLASS = NioSocketChannel.class;
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    if (EVENT_LOOP_GROUP != null) {
      EVENT_LOOP_GROUP.shutdownGracefully().syncUninterruptibly();
    }
  }

  @Override
  protected Writer createWriter(Path path) throws IOException {
    return new WriterOverAsyncWriter(AsyncFSWALProvider.createAsyncWriter(
      TEST_UTIL.getConfiguration(), fs, path, false, EVENT_LOOP_GROUP.next(), CHANNEL_CLASS));
  }
}
