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
package org.apache.hadoop.hbase.io.asyncfs;

import java.io.IOException;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;

/**
 * Helper class for creating AsyncFSOutput.
 */
@InterfaceAudience.Private
public final class AsyncFSOutputHelper {

  private AsyncFSOutputHelper() {
  }

  /**
   * Create {@link FanOutOneBlockAsyncDFSOutput} for {@link DistributedFileSystem}, and a simple
   * implementation for other {@link FileSystem} which wraps around a {@link FSDataOutputStream}.
   */
  public static AsyncFSOutput createOutput(FileSystem fs, Path f, boolean overwrite,
      boolean createParent, short replication, long blockSize, EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass)
      throws IOException, CommonFSUtils.StreamLacksCapabilityException {
    if (fs instanceof DistributedFileSystem) {
      return FanOutOneBlockAsyncDFSOutputHelper.createOutput((DistributedFileSystem) fs, f,
        overwrite, createParent, replication, blockSize, eventLoopGroup, channelClass);
    }
    final FSDataOutputStream out;
    int bufferSize = fs.getConf().getInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
      CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
    // This is not a Distributed File System, so it won't be erasure coded; no builder API needed
    if (createParent) {
      out = fs.create(f, overwrite, bufferSize, replication, blockSize, null);
    } else {
      out = fs.createNonRecursive(f, overwrite, bufferSize, replication, blockSize, null);
    }
    // After we create the stream but before we attempt to use it at all
    // ensure that we can provide the level of data safety we're configured
    // to provide.
    if (fs.getConf().getBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, true)) {
      if (!out.hasCapability(StreamCapabilities.HFLUSH)) {
        Closeables.close(out, true);
        throw new StreamLacksCapabilityException(StreamCapabilities.HFLUSH);
      }
      if (!out.hasCapability(StreamCapabilities.HSYNC)) {
        Closeables.close(out, true);
        throw new StreamLacksCapabilityException(StreamCapabilities.HSYNC);
      }
    }
    return new WrapperAsyncFSOutput(f, out);
  }
}
