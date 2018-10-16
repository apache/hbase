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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.wal.WALProvider.AsyncWriter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;

/**
 * An AsyncFSWAL which writes data to two filesystems.
 */
@InterfaceAudience.Private
public class DualAsyncFSWAL extends AsyncFSWAL {

  private static final Logger LOG = LoggerFactory.getLogger(DualAsyncFSWAL.class);

  private final FileSystem remoteFs;

  private final Path remoteWALDir;

  private volatile boolean skipRemoteWAL = false;

  private volatile boolean markerEditOnly = false;

  public DualAsyncFSWAL(FileSystem fs, FileSystem remoteFs, Path rootDir, Path remoteWALDir,
      String logDir, String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass) throws FailedLogCloseException, IOException {
    super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix,
      eventLoopGroup, channelClass);
    this.remoteFs = remoteFs;
    this.remoteWALDir = remoteWALDir;
  }

  // will be overridden in testcase
  @VisibleForTesting
  protected AsyncWriter createCombinedAsyncWriter(AsyncWriter localWriter,
      AsyncWriter remoteWriter) {
    return CombinedAsyncWriter.create(remoteWriter, localWriter);
  }

  @Override
  protected AsyncWriter createWriterInstance(Path path) throws IOException {
    AsyncWriter localWriter = super.createWriterInstance(path);
    // retry forever if we can not create the remote writer to prevent aborting the RS due to log
    // rolling error, unless the skipRemoteWal is set to true.
    // TODO: since for now we only have one thread doing log rolling, this may block the rolling for
    // other wals
    Path remoteWAL = new Path(remoteWALDir, path.getName());
    for (int retry = 0;; retry++) {
      if (skipRemoteWAL) {
        return localWriter;
      }
      AsyncWriter remoteWriter;
      try {
        remoteWriter = createAsyncWriter(remoteFs, remoteWAL);
      } catch (IOException e) {
        LOG.warn("create remote writer {} failed, retry = {}", remoteWAL, retry, e);
        try {
          Thread.sleep(ConnectionUtils.getPauseTime(100, retry));
        } catch (InterruptedException ie) {
          // restore the interrupt state
          Thread.currentThread().interrupt();
          Closeables.close(localWriter, true);
          throw (IOException) new InterruptedIOException().initCause(ie);
        }
        continue;
      }
      return createCombinedAsyncWriter(localWriter, remoteWriter);
    }
  }

  @Override
  protected boolean markerEditOnly() {
    return markerEditOnly;
  }

  // Allow temporarily skipping the creation of remote writer. When failing to write to the remote
  // dfs cluster, we need to reopen the regions and switch to use the original wal writer. But we
  // need to write a close marker when closing a region, and if it fails, the whole rs will abort.
  // So here we need to skip the creation of remote writer and make it possible to write the region
  // close marker.
  // Setting markerEdit only to true is for transiting from A to S, where we need to give up writing
  // any pending wal entries as they will be discarded. The remote cluster will replicated the
  // correct data back later. We still need to allow writing marker edits such as close region event
  // to allow closing a region.
  public void skipRemoteWAL(boolean markerEditOnly) {
    if (markerEditOnly) {
      this.markerEditOnly = true;
    }
    this.skipRemoteWAL = true;
  }
}
