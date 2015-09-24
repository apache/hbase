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
package org.apache.hadoop.hbase.io;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.fs.HFileSystem;

import com.google.common.annotations.VisibleForTesting;

/**
 * Wrapper for input stream(s) that takes care of the interaction of FS and HBase checksums,
 * as well as closing streams. Initialization is not thread-safe, but normal operation is;
 * see method comments.
 */
public class FSDataInputStreamWrapper {
  static final Log LOG = LogFactory.getLog(FSDataInputStreamWrapper.class);

  private final HFileSystem hfs;
  private final Path path;
  private final FileLink link;
  private final boolean doCloseStreams;

  /** Two stream handles, one with and one without FS-level checksum.
   * HDFS checksum setting is on FS level, not single read level, so you have to keep two
   * FS objects and two handles open to interleave different reads freely, which is very sad.
   * This is what we do:
   * 1) First, we need to read the trailer of HFile to determine checksum parameters.
   *  We always use FS checksum to do that, so ctor opens {@link #stream}.
   * 2.1) After that, if HBase checksum is not used, we'd just always use {@link #stream};
   * 2.2) If HBase checksum can be used, we'll open {@link #streamNoFsChecksum},
   *  and close {@link #stream}. User MUST call prepareForBlockReader for that to happen;
   *  if they don't, (2.1) will be the default.
   * 3) The users can call {@link #shouldUseHBaseChecksum()}, and pass its result to
   *  {@link #getStream(boolean)} to get stream (if Java had out/pointer params we could
   *  return both in one call). This stream is guaranteed to be set.
   * 4) The first time HBase checksum fails, one would call {@link #fallbackToFsChecksum(int)}.
   * That will take lock, and open {@link #stream}. While this is going on, others will
   * continue to use the old stream; if they also want to fall back, they'll also call
   * {@link #fallbackToFsChecksum(int)}, and block until {@link #stream} is set.
   * 5) After some number of checksumOk() calls, we will go back to using HBase checksum.
   * We will have 2 handles; however we presume checksums fail so rarely that we don't care.
   */
  private volatile FSDataInputStream stream = null;
  private volatile FSDataInputStream streamNoFsChecksum = null;
  private Object streamNoFsChecksumFirstCreateLock = new Object();

  // The configuration states that we should validate hbase checksums
  private boolean useHBaseChecksumConfigured;

  // Record the current state of this reader with respect to
  // validating checkums in HBase. This is originally set the same
  // value as useHBaseChecksumConfigured, but can change state as and when
  // we encounter checksum verification failures.
  private volatile boolean useHBaseChecksum;

  // In the case of a checksum failure, do these many succeeding
  // reads without hbase checksum verification.
  private volatile int hbaseChecksumOffCount = -1;

  public FSDataInputStreamWrapper(FileSystem fs, Path path) throws IOException {
    this(fs, null, path, false);
  }

  public FSDataInputStreamWrapper(FileSystem fs, Path path, boolean dropBehind) throws IOException {
    this(fs, null, path, dropBehind);
  }

  public FSDataInputStreamWrapper(FileSystem fs, FileLink link) throws IOException {
    this(fs, link, null, false);
  }
  public FSDataInputStreamWrapper(FileSystem fs, FileLink link,
                                  boolean dropBehind) throws IOException {
    this(fs, link, null, dropBehind);
  }

  private FSDataInputStreamWrapper(FileSystem fs, FileLink link,
                                   Path path, boolean dropBehind) throws IOException {
    assert (path == null) != (link == null);
    this.path = path;
    this.link = link;
    this.doCloseStreams = true;
    // If the fs is not an instance of HFileSystem, then create an instance of HFileSystem
    // that wraps over the specified fs. In this case, we will not be able to avoid
    // checksumming inside the filesystem.
    this.hfs = (fs instanceof HFileSystem) ? (HFileSystem)fs : new HFileSystem(fs);

    // Initially we are going to read the tail block. Open the reader w/FS checksum.
    this.useHBaseChecksumConfigured = this.useHBaseChecksum = false;
    this.stream = (link != null) ? link.open(hfs) : hfs.open(path);
    try {
      Class<? extends FSDataInputStream> inputStreamClass = this.stream.getClass();
      try {
        Method m = inputStreamClass.getDeclaredMethod("setDropBehind",
          new Class[] { boolean.class });
        m.invoke(stream, new Object[] { dropBehind });
      } catch (NoSuchMethodException e) {
        // Not supported, we can just ignore it
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to invoke input stream's setDropBehind method, continuing");
        }
      }
    } catch (Exception e) {
      // Skipped.
    }
  }


  /**
   * Prepares the streams for block reader. NOT THREAD SAFE. Must be called once, after any
   * reads finish and before any other reads start (what happens in reality is we read the
   * tail, then call this based on what's in the tail, then read blocks).
   * @param forceNoHBaseChecksum Force not using HBase checksum.
   */
  public void prepareForBlockReader(boolean forceNoHBaseChecksum) throws IOException {
    if (hfs == null) return;
    assert this.stream != null && !this.useHBaseChecksumConfigured;
    boolean useHBaseChecksum =
        !forceNoHBaseChecksum && hfs.useHBaseChecksum() && (hfs.getNoChecksumFs() != hfs);

    if (useHBaseChecksum) {
      FileSystem fsNc = hfs.getNoChecksumFs();
      this.streamNoFsChecksum = (link != null) ? link.open(fsNc) : fsNc.open(path);
      this.useHBaseChecksumConfigured = this.useHBaseChecksum = useHBaseChecksum;
      // Close the checksum stream; we will reopen it if we get an HBase checksum failure.
      this.stream.close();
      this.stream = null;
    }
  }

  /** For use in tests. */
  @VisibleForTesting
  public FSDataInputStreamWrapper(FSDataInputStream fsdis) {
    this(fsdis, fsdis);
  }

  /** For use in tests. */
  @VisibleForTesting
  public FSDataInputStreamWrapper(FSDataInputStream fsdis, FSDataInputStream noChecksum) {
    doCloseStreams = false;
    stream = fsdis;
    streamNoFsChecksum = noChecksum;
    path = null;
    link = null;
    hfs = null;
    useHBaseChecksumConfigured = useHBaseChecksum = false;
  }

  /**
   * @return Whether we are presently using HBase checksum.
   */
  public boolean shouldUseHBaseChecksum() {
    return this.useHBaseChecksum;
  }

  /**
   * Get the stream to use. Thread-safe.
   * @param useHBaseChecksum must be the value that shouldUseHBaseChecksum has returned
   *  at some point in the past, otherwise the result is undefined.
   */
  public FSDataInputStream getStream(boolean useHBaseChecksum) {
    return useHBaseChecksum ? this.streamNoFsChecksum : this.stream;
  }

  /**
   * Read from non-checksum stream failed, fall back to FS checksum. Thread-safe.
   * @param offCount For how many checksumOk calls to turn off the HBase checksum.
   */
  public FSDataInputStream fallbackToFsChecksum(int offCount) throws IOException {
    // checksumOffCount is speculative, but let's try to reset it less.
    boolean partOfConvoy = false;
    if (this.stream == null) {
      synchronized (streamNoFsChecksumFirstCreateLock) {
        partOfConvoy = (this.stream != null);
        if (!partOfConvoy) {
          this.stream = (link != null) ? link.open(hfs) : hfs.open(path);
        }
      }
    }
    if (!partOfConvoy) {
      this.useHBaseChecksum = false;
      this.hbaseChecksumOffCount = offCount;
    }
    return this.stream;
  }

  /** Report that checksum was ok, so we may ponder going back to HBase checksum. */
  public void checksumOk() {
    if (this.useHBaseChecksumConfigured && !this.useHBaseChecksum
        && (this.hbaseChecksumOffCount-- < 0)) {
      // The stream we need is already open (because we were using HBase checksum in the past).
      assert this.streamNoFsChecksum != null;
      this.useHBaseChecksum = true;
    }
  }

  /** Close stream(s) if necessary. */
  public void close() throws IOException {
    if (!doCloseStreams) return;
    try {
      if (stream != streamNoFsChecksum && streamNoFsChecksum != null) {
        streamNoFsChecksum.close();
        streamNoFsChecksum = null;
      }
    } finally {
      if (stream != null) {
        stream.close();
        stream = null;
      }
    }
  }

  public HFileSystem getHfs() {
    return this.hfs;
  }
}
