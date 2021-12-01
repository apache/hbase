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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutput;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper;
import org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputSaslHelper;
import org.apache.hadoop.hbase.regionserver.wal.AsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.AsyncProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.DefaultThreadFactory;

/**
 * A WAL provider that use {@link AsyncFSWAL}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AsyncFSWALProvider extends AbstractFSWALProvider<AsyncFSWAL> {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncFSWALProvider.class);

  public static final String WRITER_IMPL = "hbase.regionserver.hlog.async.writer.impl";

  // Only public so classes back in regionserver.wal can access
  public interface AsyncWriter extends WALProvider.AsyncWriter {
    /**
     * @throws IOException if something goes wrong initializing an output stream
     * @throws StreamLacksCapabilityException if the given FileSystem can't provide streams that
     *         meet the needs of the given Writer implementation.
     */
    void init(FileSystem fs, Path path, Configuration c, boolean overwritable, long blocksize)
        throws IOException, CommonFSUtils.StreamLacksCapabilityException;
  }

  private EventLoopGroup eventLoopGroup;

  private Class<? extends Channel> channelClass;
  @Override
  protected AsyncFSWAL createWAL() throws IOException {
    return new AsyncFSWAL(CommonFSUtils.getWALFileSystem(conf), this.abortable,
        CommonFSUtils.getWALRootDir(conf), getWALDirectoryName(factory.factoryId),
        getWALArchiveDirectoryName(conf, factory.factoryId), conf, listeners, true, logPrefix,
        META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null, eventLoopGroup,
        channelClass);
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
   * Public because of AsyncFSWAL. Should be package-private
   */
  public static AsyncWriter createAsyncWriter(Configuration conf, FileSystem fs, Path path,
      boolean overwritable, EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass) throws IOException {
    return createAsyncWriter(conf, fs, path, overwritable, WALUtil.getWALBlockSize(conf, fs, path),
        eventLoopGroup, channelClass);
  }

  /**
   * Public because of AsyncFSWAL. Should be package-private
   */
  public static AsyncWriter createAsyncWriter(Configuration conf, FileSystem fs, Path path,
      boolean overwritable, long blocksize, EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass) throws IOException {
    // Configuration already does caching for the Class lookup.
    Class<? extends AsyncWriter> logWriterClass = conf.getClass(
      WRITER_IMPL, AsyncProtobufLogWriter.class, AsyncWriter.class);
    try {
      AsyncWriter writer = logWriterClass.getConstructor(EventLoopGroup.class, Class.class)
          .newInstance(eventLoopGroup, channelClass);
      writer.init(fs, path, conf, overwritable, blocksize);
      return writer;
    } catch (Exception e) {
      if (e instanceof CommonFSUtils.StreamLacksCapabilityException) {
        LOG.error("The RegionServer async write ahead log provider " +
          "relies on the ability to call " + e.getMessage() + " for proper operation during " +
          "component failures, but the current FileSystem does not support doing so. Please " +
          "check the config value of '" + CommonFSUtils.HBASE_WAL_DIR + "' and ensure " +
          "it points to a FileSystem mount that has suitable capabilities for output streams.");
      } else {
        LOG.debug("Error instantiating log writer.", e);
      }
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException("cannot get log writer", e);
    }
  }

  /**
   * Test whether we can load the helper classes for async dfs output.
   */
  public static boolean load() {
    try {
      Class.forName(FanOutOneBlockAsyncDFSOutput.class.getName());
      Class.forName(FanOutOneBlockAsyncDFSOutputHelper.class.getName());
      Class.forName(FanOutOneBlockAsyncDFSOutputSaslHelper.class.getName());
      return true;
    } catch (Throwable e) {
      return false;
    }
  }
}
