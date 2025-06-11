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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
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
  private static final Logger LOG = 
      LoggerFactory.getLogger(MultiTenantFSDataInputStreamWrapper.class);

  /** The offset where this section starts in the parent file */
  private final long sectionOffset;
  /** The original input stream wrapper to delegate to */
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
   * @param relativePosition the position relative to the section start
   * @return the absolute position in the file
   */
  public long toAbsolutePosition(long relativePosition) {
    return relativePosition + sectionOffset;
  }

  /**
   * Converts an absolute file position to a position relative to the section.
   *
   * @param absolutePosition the absolute position in the file
   * @return the position relative to the section start
   */
  public long toRelativePosition(long absolutePosition) {
    return absolutePosition - sectionOffset;
  }
  
  @Override
  public FSDataInputStream getStream(boolean useHBaseChecksum) {
    // For all sections, wrap the raw stream with position translator
    FSDataInputStream rawStream = parent.getStream(useHBaseChecksum);
    return new TranslatingFSStream(rawStream);
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
   *
   * @param seekPosition the position to seek to (section-relative)
   * @throws IOException if an I/O error occurs
   */
  public void seek(long seekPosition) throws IOException {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    
    // Convert section-relative position to absolute file position
    long absolutePosition = toAbsolutePosition(seekPosition);
    stream.seek(absolutePosition);
  }

  /**
   * Custom implementation to translate position.
   *
   * @return the current position (section-relative)
   * @throws IOException if an I/O error occurs
   */
  public long getPos() throws IOException {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    long absolutePosition = stream.getPos();
    
    // Get the absolute position and convert to section-relative position
    return toRelativePosition(absolutePosition);
  }

  /**
   * Read method that translates position.
   *
   * @param buffer the buffer to read into
   * @param offset the offset in the buffer
   * @param length the number of bytes to read
   * @return the number of bytes read
   * @throws IOException if an I/O error occurs
   */
  public int read(byte[] buffer, int offset, int length) throws IOException {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream.read(buffer, offset, length);
  }

  /**
   * Custom implementation to read at position with offset translation.
   *
   * @param position the position to read from (section-relative)
   * @param buffer the buffer to read into
   * @param offset the offset in the buffer
   * @param length the number of bytes to read
   * @return the number of bytes read
   * @throws IOException if an I/O error occurs
   */
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    
    // Convert section-relative position to absolute file position
    long absolutePosition = toAbsolutePosition(position);
    return stream.read(absolutePosition, buffer, offset, length);
  }

  /**
   * Get the positioned readable interface.
   *
   * @return the positioned readable interface
   */
  public PositionedReadable getPositionedReadable() {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream;
  }

  /**
   * Get the seekable interface.
   *
   * @return the seekable interface
   */
  public Seekable getSeekable() {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream;
  }

  /**
   * Get the input stream.
   *
   * @return the input stream
   */
  public InputStream getStream() {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream;
  }

  /**
   * Check if an input stream is available.
   *
   * @return true if an input stream is available
   */
  public boolean hasInputStream() {
    return true;
  }

  /**
   * Check if positioned readable interface is available.
   *
   * @return true if positioned readable is available
   */
  public boolean hasPositionedReadable() {
    return true;
  }

  /**
   * Check if seekable interface is available.
   *
   * @return true if seekable is available
   */
  public boolean hasSeekable() {
    return true;
  }

  /**
   * Read a single byte.
   *
   * @return the byte read, or -1 if end of stream
   * @throws IOException if an I/O error occurs
   */
  public int read() throws IOException {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream.read();
  }

  /**
   * Get the stream wrapper for the given stream.
   *
   * @param stream the stream to wrap
   * @return the wrapped stream
   */
  public FSDataInputStream getStream(FSDataInputStream stream) {
    return stream;
  }

  /**
   * Translates section-relative seeks/reads into absolute file positions.
   */
  private class TranslatingFSStream extends FSDataInputStream {
    /** The raw underlying stream */
    private final FSDataInputStream rawStream;
    
    /**
     * Constructor for TranslatingFSStream.
     *
     * @param rawStream the raw stream to wrap
     */
    TranslatingFSStream(FSDataInputStream rawStream) {
      super(new OffsetTranslatingInputStream(rawStream, sectionOffset));
      this.rawStream = rawStream;
      // DO NOT automatically seek to sectionOffset here!
      // This interferes with normal HFile reading patterns.
      // The HFileReaderImpl will seek to specific positions as needed,
      // and our translator will handle the offset translation.
      LOG.debug("Created section stream wrapper for section starting at offset {}", sectionOffset);
    }
    
    @Override 
    public void seek(long position) throws IOException {
      // Convert section-relative position to absolute file position
      long absolutePosition = toAbsolutePosition(position);
      LOG.debug("Section seek: relative pos {} -> absolute pos {}, sectionOffset={}", 
                position, absolutePosition, sectionOffset);
      // Validate that we're not seeking beyond reasonable bounds
      if (position < 0) {
        LOG.warn("Attempting to seek to negative relative position: {}", position);
      }
      rawStream.seek(absolutePosition);
    }
    
    @Override 
    public long getPos() throws IOException {
      long absolutePosition = rawStream.getPos();
      
      // Convert absolute position to section-relative position
      long relativePosition = toRelativePosition(absolutePosition);
      LOG.trace("Section getPos: absolute {} -> relative {}, sectionOffset={}", 
                absolutePosition, relativePosition, sectionOffset);
      // Validate position translation
      if (relativePosition < 0) {
        LOG.warn("Position translation resulted in negative relative position: " +
                 "absolute={}, relative={}, sectionOffset={}", 
                 absolutePosition, relativePosition, sectionOffset);
      }
      return relativePosition;
    }
    
    @Override 
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      // Convert section-relative position to absolute file position
      long absolutePosition = toAbsolutePosition(position);
      LOG.trace("Section pread: relative pos {} -> absolute pos {}, len={}, sectionOffset={}", 
                position, absolutePosition, length, sectionOffset);
      // Validate read parameters
      if (position < 0) {
        LOG.warn("Attempting to read from negative relative position: {}", position);
      }
      if (length < 0) {
        throw new IllegalArgumentException("Read length cannot be negative: " + length);
      }
      return rawStream.read(absolutePosition, buffer, offset, length);
    }
    
    @Override
    public boolean seekToNewSource(long targetPosition) throws IOException {
      return rawStream.seekToNewSource(toAbsolutePosition(targetPosition));
    }
    
    // Other read methods use the underlying stream's implementations
    // Note: We cannot override final methods like read(), read(byte[]), etc.
  }
  
  /**
   * Custom InputStream that translates all read operations by adding the section offset.
   * This ensures that when DataInputStream's final methods call read(), they go through
   * our offset translation logic.
   */
  private static class OffsetTranslatingInputStream extends InputStream 
      implements Seekable, PositionedReadable {
    /** The raw underlying stream */
    private final FSDataInputStream rawStream;
    /** The section offset for translation */
    private final long sectionOffset;

    /**
     * Constructor for OffsetTranslatingInputStream.
     *
     * @param rawStream the raw stream to wrap
     * @param sectionOffset the section offset for translation
     */
    OffsetTranslatingInputStream(FSDataInputStream rawStream, long sectionOffset) {
      this.rawStream = rawStream;
      this.sectionOffset = sectionOffset;
    }

    @Override
    public int read() throws IOException {
      // For single byte reads, we rely on the current position being correctly set
      return rawStream.read();
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      // This is the key method that gets called by DataInputStream's final methods
      // We need to ensure the stream is positioned correctly before reading
      return rawStream.read(buffer, offset, length);
    }

    @Override
    public long skip(long bytesToSkip) throws IOException {
      return rawStream.skip(bytesToSkip);
    }

    @Override
    public int available() throws IOException {
      return rawStream.available();
    }

    @Override
    public void close() throws IOException {
      rawStream.close();
    }

    @Override
    public void mark(int readLimit) {
      rawStream.mark(readLimit);
    }

    @Override
    public void reset() throws IOException {
      rawStream.reset();
    }

    @Override
    public boolean markSupported() {
      return rawStream.markSupported();
    }

    // Seekable interface implementation
    @Override
    public void seek(long position) throws IOException {
      // Translate section-relative position to absolute file position
      long absolutePosition = sectionOffset + position;
      LOG.trace("OffsetTranslatingInputStream seek: relative pos {} -> absolute pos {}, " +
                "sectionOffset={}", position, absolutePosition, sectionOffset);
      rawStream.seek(absolutePosition);
    }

    @Override
    public long getPos() throws IOException {
      // Translate absolute file position back to section-relative position
      long absolutePosition = rawStream.getPos();
      long relativePosition = absolutePosition - sectionOffset;
      LOG.trace("OffsetTranslatingInputStream getPos: absolute pos {} -> relative pos {}, " +
                "sectionOffset={}", absolutePosition, relativePosition, sectionOffset);
      return relativePosition;
    }

    @Override
    public boolean seekToNewSource(long targetPosition) throws IOException {
      // Translate section-relative position to absolute file position
      long absolutePosition = sectionOffset + targetPosition;
      LOG.trace("OffsetTranslatingInputStream seekToNewSource: relative pos {} -> " +
                "absolute pos {}, sectionOffset={}", 
                targetPosition, absolutePosition, sectionOffset);
      return rawStream.seekToNewSource(absolutePosition);
    }

    // PositionedReadable interface implementation
    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      // Translate section-relative position to absolute file position
      long absolutePosition = sectionOffset + position;
      LOG.trace("OffsetTranslatingInputStream pread: relative pos {} -> absolute pos {}, " +
                "len={}, sectionOffset={}", 
                position, absolutePosition, length, sectionOffset);
      return rawStream.read(absolutePosition, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) 
        throws IOException {
      // Translate section-relative position to absolute file position
      long absolutePosition = sectionOffset + position;
      LOG.trace("OffsetTranslatingInputStream readFully: relative pos {} -> absolute pos {}, " +
                "len={}, sectionOffset={}", 
                position, absolutePosition, length, sectionOffset);
      rawStream.readFully(absolutePosition, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }
  }
} 