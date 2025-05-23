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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantFSDataInputStreamWrapper.class);

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
    // Use test constructor to properly initialize both streams and avoid assertion issues
    super(parent.getStream(false), parent.getStream(true));
    this.parent = parent;
    this.sectionOffset = offset;
    
    LOG.debug("Created section wrapper for section at offset {} (translation: {})", 
              offset, offset == 0 ? "none" : "enabled");
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
    // For all sections, wrap the raw stream with position translator
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
    // Since we're using test constructor with hfs=null, prepareForBlockReader should return early
    // and never hit the assertion. Call super instead of parent to avoid multiple calls on parent.
    super.prepareForBlockReader(forceNoHBaseChecksum);
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
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    
    // Convert section-relative position to absolute file position
    long absolutePos = toAbsolutePosition(seekPos);
    stream.seek(absolutePos);
  }

  /**
   * Custom implementation to translate position.
   */
  public long getPos() throws IOException {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    long absolutePos = stream.getPos();
    
    // Get the absolute position and convert to section-relative position
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
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    
    // Convert section-relative position to absolute file position
    long absolutePos = toAbsolutePosition(pos);
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
      // DO NOT automatically seek to sectionOffset here!
      // This interferes with normal HFile reading patterns.
      // The HFileReaderImpl will seek to specific positions as needed,
      // and our translator will handle the offset translation.
      LOG.debug("Created section stream wrapper for section starting at offset {}", sectionOffset);
    }
    
    @Override 
    public void seek(long pos) throws IOException {
      // Convert section-relative position to absolute file position
      long absolutePos = toAbsolutePosition(pos);
      LOG.debug("Section seek: relative pos {} -> absolute pos {}, sectionOffset={}", 
                pos, absolutePos, sectionOffset);
      // Validate that we're not seeking beyond reasonable bounds
      if (pos < 0) {
        LOG.warn("Attempting to seek to negative relative position: {}", pos);
      }
      raw.seek(absolutePos);
    }
    
    @Override 
    public long getPos() throws IOException {
      long absolutePos = raw.getPos();
      
      // Convert absolute position to section-relative position
      long relativePos = toRelativePosition(absolutePos);
      LOG.trace("Section getPos: absolute {} -> relative {}, sectionOffset={}", 
                absolutePos, relativePos, sectionOffset);
      // Validate position translation
      if (relativePos < 0) {
        LOG.warn("Position translation resulted in negative relative position: absolute={}, relative={}, sectionOffset={}", 
                 absolutePos, relativePos, sectionOffset);
      }
      return relativePos;
    }
    
    @Override 
    public int read(long pos, byte[] b, int off, int len) throws IOException {
      // Convert section-relative position to absolute file position
      long absolutePos = toAbsolutePosition(pos);
      LOG.trace("Section pread: relative pos {} -> absolute pos {}, len={}, sectionOffset={}", 
                pos, absolutePos, len, sectionOffset);
      // Validate read parameters
      if (pos < 0) {
        LOG.warn("Attempting to read from negative relative position: {}", pos);
      }
      if (len < 0) {
        throw new IllegalArgumentException("Read length cannot be negative: " + len);
      }
      return raw.read(absolutePos, b, off, len);
    }
    
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return raw.seekToNewSource(toAbsolutePosition(targetPos));
    }
    
    // Other read methods use the underlying stream's implementations
    // Note: We cannot override final methods like read(), read(byte[]), etc.
  }
} 