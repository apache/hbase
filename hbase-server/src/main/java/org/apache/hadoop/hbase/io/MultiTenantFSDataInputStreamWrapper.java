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
package org.apache.hadoop.hbase.io;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementation of {@link FSDataInputStreamWrapper} that adds offset translation
 * capability for multi-tenant HFiles. This allows each tenant section to have its
 * own coordinate system starting from 0, while the actual file positions are
 * calculated by adding the section offset.
 * <p>
 * The class transparently handles all position-related operations including:
 * <ul>
 *   <li>Converting relative positions (0-based within section) to absolute file positions</li>
 *   <li>Maintaining correct logical position tracking for the section reader</li>
 *   <li>Seeking and position reporting that is section-relative</li>
 * </ul>
 */
@InterfaceAudience.Private
public class MultiTenantFSDataInputStreamWrapper extends FSDataInputStreamWrapper {
  // The offset where this section starts in the parent file
  private final long sectionOffset;
  private final FSDataInputStreamWrapper parent;

  /**
   * Constructor that creates a wrapper with offset translation.
   *
   * @param parent the original input stream wrapper to delegate to
   * @param offset the offset where the section starts in the parent file
   */
  public MultiTenantFSDataInputStreamWrapper(FSDataInputStreamWrapper parent, long offset) {
    super(parent.getStream(false));
    this.parent = parent;
    this.sectionOffset = offset;
  }

  /**
   * Converts a position relative to the section to an absolute file position.
   *
   * @param relativePos the position relative to the section start
   * @return the absolute position in the file
   */
  public long toAbsolutePosition(long relativePos) {
    return relativePos + sectionOffset;
  }

  /**
   * Converts an absolute file position to a position relative to the section.
   *
   * @param absolutePos the absolute position in the file
   * @return the position relative to the section start
   */
  public long toRelativePosition(long absolutePos) {
    return absolutePos - sectionOffset;
  }

  @Override
  public FSDataInputStream getStream(boolean useHBaseChecksum) {
    // Always wrap the raw stream so each call uses fresh translator
    FSDataInputStream raw = parent.getStream(useHBaseChecksum);
    return new TranslatingFSStream(raw);
  }

  @Override
  public Path getReaderPath() {
    return parent.getReaderPath();
  }

  @Override
  public boolean shouldUseHBaseChecksum() {
    return parent.shouldUseHBaseChecksum();
  }

  @Override
  public void prepareForBlockReader(boolean forceNoHBaseChecksum) throws IOException {
    parent.prepareForBlockReader(forceNoHBaseChecksum);
  }

  @Override
  public FSDataInputStream fallbackToFsChecksum(int offCount) throws IOException {
    return parent.fallbackToFsChecksum(offCount);
  }

  @Override
  public void checksumOk() {
    parent.checksumOk();
  }

  @Override
  public void unbuffer() {
    parent.unbuffer();
  }

  @Override
  public void close() {
    // Keep parent.close() behavior (do not close parent stream here)
  }

  /**
   * Custom implementation to translate seek position.
   */
  public void seek(long seekPos) throws IOException {
    // Convert section-relative position to absolute file position
    long absolutePos = toAbsolutePosition(seekPos);
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    stream.seek(absolutePos);
  }

  /**
   * Custom implementation to translate position.
   */
  public long getPos() throws IOException {
    // Get the absolute position and convert to section-relative position
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    long absolutePos = stream.getPos();
    return toRelativePosition(absolutePos);
  }

  /**
   * Read method that translates position.
   */
  public int read(byte[] b, int off, int len) throws IOException {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream.read(b, off, len);
  }

  /**
   * Custom implementation to read at position with offset translation.
   */
  public int read(long pos, byte[] b, int off, int len) throws IOException {
    long absolutePos = toAbsolutePosition(pos);
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream.read(absolutePos, b, off, len);
  }

  public PositionedReadable getPositionedReadable() {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream;
  }

  public Seekable getSeekable() {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream;
  }

  public InputStream getStream() {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream;
  }

  public boolean hasInputStream() {
    return true;
  }

  public boolean hasPositionedReadable() {
    return true;
  }

  public boolean hasSeekable() {
    return true;
  }

  public int read() throws IOException {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream.read();
  }

  public FSDataInputStream getStream(FSDataInputStream stream) {
    return stream;
  }

  /**
   * Translates section-relative seeks/reads into absolute file positions.
   */
  private class TranslatingFSStream extends FSDataInputStream {
    private final FSDataInputStream raw;
    TranslatingFSStream(FSDataInputStream raw) {
      super(raw.getWrappedStream());
      this.raw = raw;
    }
    @Override public void seek(long pos) throws IOException { raw.seek(toAbsolutePosition(pos)); }
    @Override public long getPos() throws IOException { return toRelativePosition(raw.getPos()); }
    @Override public int read(long pos, byte[] b, int off, int len) throws IOException {
      return raw.read(toAbsolutePosition(pos), b, off, len);
    }
    // Other read()/read(b,off,len) use default implementations after seek
  }
} 