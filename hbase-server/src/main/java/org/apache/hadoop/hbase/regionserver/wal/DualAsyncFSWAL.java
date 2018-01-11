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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.wal.WALProvider.AsyncWriter;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;

/**
 * An AsyncFSWAL which writes data to two filesystems.
 */
@InterfaceAudience.Private
public class DualAsyncFSWAL extends AsyncFSWAL {

  private final FileSystem remoteFs;

  private final Path remoteWalDir;

  public DualAsyncFSWAL(FileSystem fs, FileSystem remoteFs, Path rootDir, Path remoteRootDir,
      String logDir, String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass) throws FailedLogCloseException, IOException {
    super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix,
        eventLoopGroup, channelClass);
    this.remoteFs = remoteFs;
    this.remoteWalDir = new Path(remoteRootDir, logDir);
  }

  @Override
  protected AsyncWriter createWriterInstance(Path path) throws IOException {
    AsyncWriter localWriter = super.createWriterInstance(path);
    AsyncWriter remoteWriter;
    boolean succ = false;
    try {
      remoteWriter = createAsyncWriter(remoteFs, new Path(remoteWalDir, path.getName()));
      succ = true;
    } finally {
      if (!succ) {
        closeWriter(localWriter);
      }
    }
    return CombinedAsyncWriter.create(CombinedAsyncWriter.Mode.SEQUENTIAL, remoteWriter,
      localWriter);
  }
}
