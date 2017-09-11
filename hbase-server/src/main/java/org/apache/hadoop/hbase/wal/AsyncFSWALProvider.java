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
package org.apache.hadoop.hbase.wal;

import org.apache.hadoop.hbase.shaded.com.google.common.base.Throwables;

import org.apache.hadoop.hbase.shaded.io.netty.channel.Channel;
import org.apache.hadoop.hbase.shaded.io.netty.channel.EventLoop;
import org.apache.hadoop.hbase.shaded.io.netty.channel.EventLoopGroup;
import org.apache.hadoop.hbase.shaded.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hadoop.hbase.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hadoop.hbase.shaded.io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.wal.AsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.AsyncProtobufLogWriter;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;

/**
 * A WAL provider that use {@link AsyncFSWAL}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AsyncFSWALProvider extends AbstractFSWALProvider<AsyncFSWAL> {

  private static final Log LOG = LogFactory.getLog(AsyncFSWALProvider.class);

  // Only public so classes back in regionserver.wal can access
  public interface AsyncWriter extends WALProvider.AsyncWriter {
    void init(FileSystem fs, Path path, Configuration c, boolean overwritable) throws IOException;
  }

  private EventLoopGroup eventLoopGroup;

  private Class<? extends Channel> channelClass;
  @Override
  protected AsyncFSWAL createWAL() throws IOException {
    return new AsyncFSWAL(FSUtils.getWALFileSystem(conf), FSUtils.getWALRootDir(conf),
        getWALDirectoryName(factory.factoryId), HConstants.HREGION_OLDLOGDIR_NAME, conf, listeners,
        true, logPrefix, META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null,
        eventLoopGroup.next(), channelClass);
  }

  @Override
  protected void doInit(Configuration conf) throws IOException {
    Pair<EventLoopGroup, Class<? extends Channel>> eventLoopGroupAndChannelClass =
        NettyAsyncFSWALConfigHelper.getEventLoopConfig(conf);
    if (eventLoopGroupAndChannelClass != null) {
      eventLoopGroup = eventLoopGroupAndChannelClass.getFirst();
      channelClass = eventLoopGroupAndChannelClass.getSecond();
    } else {
      eventLoopGroup = new NioEventLoopGroup(1,
          new DefaultThreadFactory("AsyncFSWAL", true, Thread.MAX_PRIORITY));
      channelClass = NioSocketChannel.class;
    }
  }

  /**
   * public because of AsyncFSWAL. Should be package-private
   */
  public static AsyncWriter createAsyncWriter(Configuration conf, FileSystem fs, Path path,
      boolean overwritable, EventLoop eventLoop, Class<? extends Channel> channelClass)
      throws IOException {
    // Configuration already does caching for the Class lookup.
    Class<? extends AsyncWriter> logWriterClass = conf.getClass(
      "hbase.regionserver.hlog.async.writer.impl", AsyncProtobufLogWriter.class, AsyncWriter.class);
    try {
      AsyncWriter writer = logWriterClass.getConstructor(EventLoop.class, Class.class)
          .newInstance(eventLoop, channelClass);
      writer.init(fs, path, conf, overwritable);
      return writer;
    } catch (Exception e) {
      LOG.debug("Error instantiating log writer.", e);
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException("cannot get log writer", e);
    }
  }
}
