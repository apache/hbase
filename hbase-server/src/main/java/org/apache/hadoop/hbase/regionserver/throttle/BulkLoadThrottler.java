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
package org.apache.hadoop.hbase.regionserver.throttle;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.io.IOUtils.closeStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.SecureBulkLoadManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bulk load throttling controller for RS: enabled if bandwidth &gt; 0, a cycle = 100ms, by
 * throttling we guarantee data pushed to peer within each cycle won't exceed 'bandwidth' bytes
 */
@InterfaceAudience.Private
public class BulkLoadThrottler {
  private static final Logger LOG = LoggerFactory.getLogger(BulkLoadThrottler.class);
  private Configuration conf;
  private boolean enabled;
  private long bandwidth;
  private long cyclePushSize;
  private long speedLastDownloadSize;
  private long cycleStartTick;
  // a cycle = 1000ms
  private static final Long CYCLE = 1000L;
  private Long currentBulkLoadBandwidth;

  /**
   * BulkLoadThrottler constructor If bandwidth less than 1, throttling is disabled
   * @param bandwidth bandwidth cycle(1000ms)
   */
  public BulkLoadThrottler(final long bandwidth, Configuration conf) {
    this.conf = conf;
    this.bandwidth = bandwidth;
    LOG.info("Init bulk load bandwidth is {}", bandwidth);
    this.enabled = this.bandwidth > 0;
    if (this.enabled) {
      this.cyclePushSize = 0;
      this.cycleStartTick = EnvironmentEdgeManager.currentTime();
    }
    currentBulkLoadBandwidth = conf.getLong(SecureBulkLoadManager.HBASE_BULKLOAD_NODE_BANDWIDTH,
      SecureBulkLoadManager.DEFAULT_HBASE_BULKLOAD_NODE_BANDWIDTH);
  }

  public void setConf(Configuration newConf) {
    this.conf = newConf;
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * If bulk load throttling is enabled
   * @return true if bulk load throttling is enabled
   */
  public boolean isEnabled() {
    return this.enabled;
  }

  public synchronized void limitNextBytes(final int size) {
    if (!this.enabled) {
      return;
    }
    addPushSize(size);
    long now = EnvironmentEdgeManager.currentTime();
    if (cyclePushSize - speedLastDownloadSize >= bandwidth) {
      long interval = now - cycleStartTick;
      if (interval < CYCLE) {
        long sleepTicks = CYCLE - interval;
        if (sleepTicks > 0) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("To sleep {}ms for bulk load throttling control ", sleepTicks);
          }
          try {
            Thread.sleep(sleepTicks);
          } catch (InterruptedException e) {
            LOG.error(
              "Interrupted while sleeping for throttling control, " + "currentByteSize is {}", size,
              e);
          }
        }
      }
      speedLastDownloadSize = cyclePushSize;
      resetStartTick();
    } else {
      long nextCycleTick = this.cycleStartTick + CYCLE;
      if (now > nextCycleTick) {
        this.cycleStartTick = now;
      }
    }
  }

  /**
   * Add current size to the current cycle's total push size
   * @param size is the current size added to the current cycle's total push size
   */
  private void addPushSize(final int size) {
    if (this.enabled) {
      this.cyclePushSize += size;
    }
  }

  /**
   * Reset the cycle start tick to NOW
   */
  public void resetStartTick() {
    if (this.enabled) {
      this.cycleStartTick = EnvironmentEdgeManager.currentTime();
    }
  }

  /**
   * Set bulk load Bandwidth throttling
   * @param newBandwidth set bandwidth size
   */

  public void setBandwidth(long newBandwidth) {
    LOG.info("Bandwidth change {}Byte/sec to {}Byte/sec", this.bandwidth, newBandwidth);
    this.bandwidth = newBandwidth;
    this.enabled = this.bandwidth > 0;
  }

  /**
   * Get bulk load Bandwidth throttling
   * @return get bandwidth size
   */
  public long getBandwidth() {
    return bandwidth;
  }

  private void tryThrottle(int batchSize) {
    checkBulkLoadBandwidthChangeAndResetThrottler();
    if (isEnabled()) {
      limitNextBytes(batchSize);
    }
  }

  public void checkBulkLoadBandwidthChangeAndResetThrottler() {
    long Bandwidth = getCurrentBandwidth();
    if (Bandwidth != currentBulkLoadBandwidth) {
      LOG.info("Bulk load node bandwidth throttling changed, {}Byte/sec to {}Byte/sec",
        currentBulkLoadBandwidth, Bandwidth);
      currentBulkLoadBandwidth = Bandwidth;
      setBandwidth(currentBulkLoadBandwidth);
    }
  }

  private long getCurrentBandwidth() {
    long tableBandwidth = conf.getLong(SecureBulkLoadManager.HBASE_BULKLOAD_NODE_BANDWIDTH,
      SecureBulkLoadManager.DEFAULT_HBASE_BULKLOAD_NODE_BANDWIDTH);
    return tableBandwidth >= 0 ? tableBandwidth : 0;
  }

  public boolean copy(FileSystem srcFS, Path src, FileSystem dstFS, Path dst, boolean deleteSource,
    Configuration conf) throws IOException {
    return copy(srcFS, srcFS.getFileStatus(src), dstFS, dst, deleteSource, true, conf);
  }

  /**
   * Copy files between FileSystems.
   */
  private boolean copy(FileSystem srcFS, FileStatus srcStatus, FileSystem dstFS, Path dst,
    boolean deleteSource, boolean overwrite, Configuration conf) throws IOException {
    Path src = srcStatus.getPath();
    dst = checkDest(src.getName(), dstFS, dst, overwrite);
    if (srcStatus.isDirectory()) {
      checkDependencies(srcFS, src, dstFS, dst);
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      FileStatus[] contents = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i], dstFS, new Path(dst, contents[i].getPath().getName()),
          deleteSource, overwrite, conf);
      }
    } else {
      InputStream in = null;
      OutputStream out = null;
      try {
        in = srcFS.open(src);
        out = dstFS.create(dst, overwrite);
        copyBytes(in, out, conf, true);
      } catch (IOException e) {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        throw e;
      }
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }

  }

  private Path checkDest(String srcName, FileSystem dstFS, Path dst, boolean overwrite)
    throws IOException {
    if (dstFS.exists(dst)) {
      FileStatus sdst = dstFS.getFileStatus(dst);
      if (sdst.isDirectory()) {
        if (null == srcName) {
          throw new IOException("Target " + dst + " is a directory");
        }
        return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
      } else if (!overwrite) {
        throw new IOException("Target " + dst + " already exists");
      }
    }
    return dst;
  }

  // If the destination is a subdirectory of the source, then generate exception
  private void checkDependencies(FileSystem srcFS, Path src, FileSystem dstFS, Path dst)
    throws IOException {
    if (srcFS == dstFS) {
      String srcq = src.makeQualified(srcFS).toString() + Path.SEPARATOR;
      String dstq = dst.makeQualified(dstFS).toString() + Path.SEPARATOR;
      if (dstq.startsWith(srcq)) {
        if (srcq.length() == dstq.length()) {
          throw new IOException("Cannot copy " + src + " to itself.");
        } else {
          throw new IOException("Cannot copy " + src + " to its subdirectory " + dst);
        }
      }
    }
  }

  private void copyBytes(InputStream in, OutputStream out, Configuration conf, boolean close)
    throws IOException {
    copyBytes(in, out, conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), close);
  }

  private void copyBytes(InputStream in, OutputStream out, int buffSize, boolean close)
    throws IOException {
    try {
      copyBytes(in, out, buffSize);
      if (close) {
        out.close();
        out = null;
        in.close();
        in = null;
      }
    } finally {
      if (close) {
        closeStream(out);
        closeStream(in);
      }
    }
  }

  private void copyBytes(InputStream in, OutputStream out, int buffSize) throws IOException {
    PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
    byte[] buf = new byte[buffSize];
    int bytesRead = in.read(buf);
    while (bytesRead >= 0) {
      tryThrottle(bytesRead);
      out.write(buf, 0, bytesRead);
      if ((ps != null) && ps.checkError()) {
        throw new IOException("Unable to write to output stream.");
      }
      bytesRead = in.read(buf);
    }
  }
}
