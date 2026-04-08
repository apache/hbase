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
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link FSDataInputStreamWrapper} that adds offset translation capability for
 * multi-tenant HFiles. This allows each tenant section to have its own coordinate system starting
 * from 0, while the actual file positions are calculated by adding the section offset.
 * <p>
 * The class transparently handles all position-related operations including:
 * <ul>
 * <li>Converting relative positions (0-based within section) to absolute file positions</li>
 * <li>Maintaining correct logical position tracking for the section reader</li>
 * <li>Seeking and position reporting that is section-relative</li>
 * </ul>
 */
@InterfaceAudience.Private
public class MultiTenantFSDataInputStreamWrapper extends FSDataInputStreamWrapper {
  private static final Logger LOG =
    LoggerFactory.getLogger(MultiTenantFSDataInputStreamWrapper.class);

  /** The offset where this section starts in the parent file */
  private final long sectionOffset;
  /** The size of this section in bytes; used for boundary enforcement */
  private final long sectionSize;
  /** The original input stream wrapper to delegate to */
  private final FSDataInputStreamWrapper parent;
  /** Whether this wrapper owns the parent and must close it */
  private final boolean ownsParent;
  /** Cached translating wrappers to avoid per-call allocation (one per checksum mode) */
  private volatile TranslatingFSStream cachedStreamWithChecksum;
  private volatile TranslatingFSStream cachedStreamWithoutChecksum;

  /**
   * Constructor that creates a wrapper with offset translation and boundary enforcement. The parent
   * stream is shared (not owned) — it will NOT be closed when this wrapper is closed.
   * @param parent      the original input stream wrapper to delegate to
   * @param offset      the offset where the section starts in the parent file
   * @param sectionSize the size of the section in bytes
   */
  public MultiTenantFSDataInputStreamWrapper(FSDataInputStreamWrapper parent, long offset,
    long sectionSize) {
    this(parent, offset, sectionSize, false);
  }

  /**
   * Constructor that creates a wrapper with offset translation and boundary enforcement.
   * @param parent      the original input stream wrapper to delegate to
   * @param offset      the offset where the section starts in the parent file
   * @param sectionSize the size of the section in bytes
   * @param ownsParent  if true, closing this wrapper also closes the parent stream. Use true when
   *                    the parent was opened exclusively for this section (e.g., stream readers).
   */
  public MultiTenantFSDataInputStreamWrapper(FSDataInputStreamWrapper parent, long offset,
    long sectionSize, boolean ownsParent) {
    super(java.util.Objects.requireNonNull(parent, "parent stream wrapper").getStream(false),
      parent.getStream(true));
    if (offset < 0) {
      throw new IllegalArgumentException("Section offset must be non-negative, got: " + offset);
    }
    if (sectionSize <= 0) {
      throw new IllegalArgumentException("Section size must be positive, got: " + sectionSize);
    }
    this.parent = parent;
    this.sectionOffset = offset;
    this.sectionSize = sectionSize;
    this.ownsParent = ownsParent;

    LOG.debug("Created section wrapper at offset {}, size {} (ownsParent={}, translation: {})",
      offset, sectionSize, ownsParent, offset == 0 ? "none" : "enabled");
  }

  /**
   * Converts a position relative to the section to an absolute file position.
   * @param relativePosition the position relative to the section start
   * @return the absolute position in the file
   */
  public long toAbsolutePosition(long relativePosition) {
    return relativePosition + sectionOffset;
  }

  /**
   * Converts an absolute file position to a position relative to the section.
   * @param absolutePosition the absolute position in the file
   * @return the position relative to the section start
   */
  public long toRelativePosition(long absolutePosition) {
    return absolutePosition - sectionOffset;
  }

  /**
   * Validates that a section-relative position is within the section bounds.
   * @param position the section-relative position to validate
   * @throws IOException if the position is outside the section
   */
  private void checkSeekBounds(long position) throws IOException {
    if (position < 0 || position > sectionSize) {
      throw new IOException("Seek position " + position + " is outside section bounds [0, "
        + sectionSize + "], sectionOffset=" + sectionOffset);
    }
  }

  /**
   * Validates that a positioned read stays within the section and returns the clamped length.
   * @param position the section-relative start position
   * @param length   the requested read length
   * @return the clamped length that stays within the section
   * @throws IOException if the position is outside the section
   */
  private int clampReadLength(long position, int length) throws IOException {
    if (position < 0 || position >= sectionSize) {
      throw new IOException("Read position " + position + " is outside section bounds [0, "
        + sectionSize + "), sectionOffset=" + sectionOffset);
    }
    long remaining = sectionSize - position;
    return (int) Math.min(length, remaining);
  }

  @Override
  public FSDataInputStream getStream(boolean useHBaseChecksum) {
    FSDataInputStream rawStream = parent.getStream(useHBaseChecksum);
    if (useHBaseChecksum) {
      TranslatingFSStream cached = cachedStreamWithChecksum;
      if (cached == null || cached.rawStream != rawStream) {
        cached = new TranslatingFSStream(rawStream);
        cachedStreamWithChecksum = cached;
      }
      return cached;
    } else {
      TranslatingFSStream cached = cachedStreamWithoutChecksum;
      if (cached == null || cached.rawStream != rawStream) {
        cached = new TranslatingFSStream(rawStream);
        cachedStreamWithoutChecksum = cached;
      }
      return cached;
    }
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
    return new TranslatingFSStream(parent.fallbackToFsChecksum(offCount));
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
    if (ownsParent) {
      parent.close();
    }
  }

  /**
   * Custom implementation to translate seek position.
   * @param seekPosition the position to seek to (section-relative)
   * @throws IOException if an I/O error occurs
   */
  public void seek(long seekPosition) throws IOException {
    checkSeekBounds(seekPosition);
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());

    // Convert section-relative position to absolute file position
    long absolutePosition = toAbsolutePosition(seekPosition);
    stream.seek(absolutePosition);
  }

  /**
   * Custom implementation to translate position.
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
   * @param position the position to read from (section-relative)
   * @param buffer   the buffer to read into
   * @param offset   the offset in the buffer
   * @param length   the number of bytes to read
   * @return the number of bytes read
   * @throws IOException if an I/O error occurs
   */
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    int clampedLength = clampReadLength(position, length);
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());

    // Convert section-relative position to absolute file position
    long absolutePosition = toAbsolutePosition(position);
    return stream.read(absolutePosition, buffer, offset, clampedLength);
  }

  /**
   * Get the positioned readable interface.
   * @return the positioned readable interface
   */
  public PositionedReadable getPositionedReadable() {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream;
  }

  /**
   * Get the seekable interface.
   * @return the seekable interface
   */
  public Seekable getSeekable() {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream;
  }

  /**
   * Get the input stream.
   * @return the input stream
   */
  public InputStream getStream() {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream;
  }

  /**
   * Check if an input stream is available.
   * @return true if an input stream is available
   */
  public boolean hasInputStream() {
    return true;
  }

  /**
   * Check if positioned readable interface is available.
   * @return true if positioned readable is available
   */
  public boolean hasPositionedReadable() {
    return true;
  }

  /**
   * Check if seekable interface is available.
   * @return true if seekable is available
   */
  public boolean hasSeekable() {
    return true;
  }

  /**
   * Read a single byte.
   * @return the byte read, or -1 if end of stream
   * @throws IOException if an I/O error occurs
   */
  public int read() throws IOException {
    FSDataInputStream stream = parent.getStream(shouldUseHBaseChecksum());
    return stream.read();
  }

  /**
   * Get the stream wrapper for the given stream.
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
     * @param rawStream the raw stream to wrap
     */
    TranslatingFSStream(FSDataInputStream rawStream) {
      super(new OffsetTranslatingInputStream(rawStream, sectionOffset, sectionSize));
      this.rawStream = rawStream;
      // DO NOT automatically seek to sectionOffset here!
      // This interferes with normal HFile reading patterns.
      // The HFileReaderImpl will seek to specific positions as needed,
      // and our translator will handle the offset translation.
      LOG.debug("Created section stream wrapper for section starting at offset {}", sectionOffset);
    }

    @Override
    public void seek(long position) throws IOException {
      checkSeekBounds(position);
      long absolutePosition = toAbsolutePosition(position);
      LOG.debug("Section seek: relative pos {} -> absolute pos {}, sectionOffset={}", position,
        absolutePosition, sectionOffset);
      rawStream.seek(absolutePosition);
    }

    @Override
    public long getPos() throws IOException {
      long absolutePosition = rawStream.getPos();

      // Convert absolute position to section-relative position
      long relativePosition = toRelativePosition(absolutePosition);
      LOG.trace("Section getPos: absolute {} -> relative {}, sectionOffset={}", absolutePosition,
        relativePosition, sectionOffset);
      if (relativePosition < 0) {
        throw new IOException(
          "Position translation resulted in negative relative position: absolute="
            + absolutePosition + ", sectionOffset=" + sectionOffset);
      }
      return relativePosition;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      int clampedLength = clampReadLength(position, length);
      long absolutePosition = toAbsolutePosition(position);
      LOG.trace("Section pread: relative pos {} -> absolute pos {}, len={}, sectionOffset={}",
        position, absolutePosition, clampedLength, sectionOffset);
      return rawStream.read(absolutePosition, buffer, offset, clampedLength);
    }

    @Override
    public boolean seekToNewSource(long targetPosition) throws IOException {
      checkSeekBounds(targetPosition);
      return rawStream.seekToNewSource(toAbsolutePosition(targetPosition));
    }

    @Override
    public boolean hasCapability(String capability) {
      return rawStream.hasCapability(capability);
    }

    @Override
    public int read(long position, ByteBuffer buf) throws IOException {
      int clampedLimit = clampReadLength(position, buf.remaining());
      long absolutePosition = toAbsolutePosition(position);
      if (clampedLimit < buf.remaining()) {
        ByteBuffer limited = buf.duplicate();
        limited.limit(limited.position() + clampedLimit);
        return rawStream.read(absolutePosition, limited);
      }
      return rawStream.read(absolutePosition, buf);
    }
  }

  /**
   * Custom InputStream that translates all read operations by adding the section offset. This
   * ensures that when DataInputStream's final methods call read(), they go through our offset
   * translation logic.
   */
  private static class OffsetTranslatingInputStream extends InputStream
    implements Seekable, PositionedReadable {
    /** The raw underlying stream */
    private final FSDataInputStream rawStream;
    /** The section offset for translation */
    private final long sectionOffset;
    /** The section size for boundary enforcement */
    private final long sectionSize;
    /** Absolute end boundary: reads must not go beyond this position */
    private final long sectionEndAbsolute;

    /**
     * Constructor for OffsetTranslatingInputStream.
     * @param rawStream     the raw stream to wrap
     * @param sectionOffset the section offset for translation
     * @param sectionSize   the section size for boundary enforcement
     */
    OffsetTranslatingInputStream(FSDataInputStream rawStream, long sectionOffset,
      long sectionSize) {
      this.rawStream = rawStream;
      this.sectionOffset = sectionOffset;
      this.sectionSize = sectionSize;
      this.sectionEndAbsolute = sectionOffset + sectionSize;
    }

    private long remainingInSection() throws IOException {
      return sectionEndAbsolute - rawStream.getPos();
    }

    @Override
    public int read() throws IOException {
      if (remainingInSection() <= 0) {
        return -1;
      }
      return rawStream.read();
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      long remaining = remainingInSection();
      if (remaining <= 0) {
        return -1;
      }
      int clampedLength = (int) Math.min(length, remaining);
      return rawStream.read(buffer, offset, clampedLength);
    }

    @Override
    public long skip(long bytesToSkip) throws IOException {
      long remaining = remainingInSection();
      if (remaining <= 0) {
        return 0;
      }
      long clampedSkip = Math.min(bytesToSkip, remaining);
      return rawStream.skip(clampedSkip);
    }

    @Override
    public int available() throws IOException {
      return rawStream.available();
    }

    @Override
    public void close() throws IOException {
      // Do not close the shared raw stream. Section readers are lightweight views that
      // should not own the lifecycle of the underlying stream.
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
      if (position < 0 || position > sectionSize) {
        throw new IOException("Seek position " + position + " is outside section bounds [0, "
          + sectionSize + "], sectionOffset=" + sectionOffset);
      }
      long absolutePosition = sectionOffset + position;
      LOG.trace("OffsetTranslatingInputStream seek: relative pos {} -> absolute pos {}, "
        + "sectionOffset={}", position, absolutePosition, sectionOffset);
      rawStream.seek(absolutePosition);
    }

    @Override
    public long getPos() throws IOException {
      // Translate absolute file position back to section-relative position
      long absolutePosition = rawStream.getPos();
      long relativePosition = absolutePosition - sectionOffset;
      LOG.trace("OffsetTranslatingInputStream getPos: absolute pos {} -> relative pos {}, "
        + "sectionOffset={}", absolutePosition, relativePosition, sectionOffset);
      return relativePosition;
    }

    @Override
    public boolean seekToNewSource(long targetPosition) throws IOException {
      if (targetPosition < 0 || targetPosition > sectionSize) {
        throw new IOException("seekToNewSource position " + targetPosition
          + " is outside section bounds [0, " + sectionSize + "], sectionOffset=" + sectionOffset);
      }
      long absolutePosition = sectionOffset + targetPosition;
      LOG.trace("OffsetTranslatingInputStream seekToNewSource: relative pos {} -> "
        + "absolute pos {}, sectionOffset={}", targetPosition, absolutePosition, sectionOffset);
      return rawStream.seekToNewSource(absolutePosition);
    }

    // PositionedReadable interface implementation
    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      if (position < 0 || position >= sectionSize) {
        throw new IOException("Read position " + position + " is outside section bounds [0, "
          + sectionSize + "), sectionOffset=" + sectionOffset);
      }
      int clampedLength = (int) Math.min(length, sectionSize - position);
      long absolutePosition = sectionOffset + position;
      LOG.trace("OffsetTranslatingInputStream pread: relative pos {} -> absolute pos {}, "
        + "len={}, sectionOffset={}", position, absolutePosition, clampedLength, sectionOffset);
      return rawStream.read(absolutePosition, buffer, offset, clampedLength);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      if (position < 0 || position + length > sectionSize) {
        throw new IOException("readFully range [" + position + ", " + (position + length)
          + ") is outside section bounds [0, " + sectionSize + "), sectionOffset=" + sectionOffset);
      }
      long absolutePosition = sectionOffset + position;
      LOG.trace("OffsetTranslatingInputStream readFully: relative pos {} -> absolute pos {}, "
        + "len={}, sectionOffset={}", position, absolutePosition, length, sectionOffset);
      rawStream.readFully(absolutePosition, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }
  }
}
