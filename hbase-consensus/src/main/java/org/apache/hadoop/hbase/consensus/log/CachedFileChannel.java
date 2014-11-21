package org.apache.hadoop.hbase.consensus.log;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CachedFileChannel extends FileChannel {
  private RandomAccessFile raf;
  private long startPosMBFF = -1;
  private long countMBFF = 0;
  private long curPosition = 0;
  private MappedByteBuffer mBff = null;
  private FileChannel fileChannel;
  private long maxSizeToPrefetch = 1000000;
  public static final Log LOG = LogFactory.getLog(CachedFileChannel.class);

  public CachedFileChannel(RandomAccessFile raf, long countMBFF) {
    this.raf = raf;
    maxSizeToPrefetch = countMBFF;

    this.fileChannel = this.raf.getChannel();
  }

  /**
   * Tries to read from the mapped buffer.
   *
   * If the requested range is already mapped, we can read it right away. If not,
   * the method will try to map the portion that is being read, if the size is smaller
   * than the MAX_SIZE. Otherwise, the requested size is too big, or it is 0, we return
   * false. (The caller will then have to fetch the data from the fileChannel)
   *
   * @param buffer
   * @param fileOffset
   * @param size
   * @return true if the read was successful. false otherwise.
   * @throws IOException
   */
  private boolean tryReadingFromMemoryBuffer(ByteBuffer buffer,
      long fileOffset, int size) throws IOException {
    if (size > maxSizeToPrefetch
        || size <= 0
        || fileOffset + size > size()) {
      LOG.debug("Did not preload " + size
          + "  bytes from log file starting at offset " + fileOffset
          + ". Memory currently contains: " + this.startPosMBFF + " to "
          + (this.startPosMBFF + this.countMBFF) + " looking for "
          + fileOffset + " to " + (fileOffset + size)
          + " file size is " + size());
      return false;
    }

    if (fileOffset < this.startPosMBFF
        || fileOffset + size > this.startPosMBFF + this.countMBFF ) {
      loadMemoryBufferFrom(fileOffset);
    }

    readEntryFromMemoryBuffer(buffer,
        (int)(fileOffset - this.startPosMBFF),
        size);
    this.curPosition = this.startPosMBFF + this.mBff.position();

    return true;
  }

  /**
   * Map a portion of the file into the MappedByteBuffer
   * @param fileOffset offset in the file to map from
   * @throws IOException when FileChannel.map has issues
   */
  private void loadMemoryBufferFrom(long fileOffset) throws IOException {
    countMBFF = Math.min(maxSizeToPrefetch, size() - fileOffset);
    startPosMBFF = fileOffset;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Preloading " + countMBFF
          + "  bytes from log file starting at offset " + startPosMBFF);
    }
    mBff = raf.getChannel().map(MapMode.READ_ONLY, startPosMBFF, countMBFF);
  }

  /**
   * Reads the specified data from the mapped buffer into the given buffer.
   * @param bb buffer to read the data into
   * @param memOffset memory offset in the mapped buffer to start reading from.
   *        (THIS is NOT necessarily the offset in the file.)
   * @param size
   */
  private void readEntryFromMemoryBuffer(ByteBuffer bb, int memOffset, int size) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Loading into memory. Memory currently contains: "
          + startPosMBFF + " to " + (startPosMBFF + countMBFF)
          + " looking for " + (startPosMBFF + memOffset) + " to "
          + (startPosMBFF + memOffset + size));
    }
    // temporarily modify the limit to the size of data we want to read.
    int oldLimit = this.mBff.limit();
    this.mBff.position(memOffset);
    int newLimit = memOffset + size;
    this.mBff.limit(newLimit);

    bb.put(mBff);

    // restore old limit
    this.mBff.limit(oldLimit);
  }

  @Override
  public long position() throws IOException {
    return this.curPosition;
  }

  @Override
  public FileChannel position(long newPosition) throws IOException {
    this.curPosition = newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    return this.fileChannel.size();
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return read(dst, this.curPosition);
  }

  @Override
  public int read(ByteBuffer buffer, long offset) throws IOException {
    int ret = 0;
    if (!tryReadingFromMemoryBuffer(buffer, offset, buffer.remaining())) {
      // This is too large to fit in mBFF anyways

      if (offset != fileChannel.position()) {
        fileChannel.position(offset);
      }

      ret = fileChannel.read(buffer);
      this.curPosition = fileChannel.position();
    }
    return ret;
  }

  @Override
  public long read(ByteBuffer[] buffer, int offset, int length)
      throws IOException {
    throw new UnsupportedOperationException(
        "CachedFileChannel does not support this operation");
  }

  @Override
  protected void implCloseChannel() throws IOException {
    this.mBff = null;
    this.curPosition = 0;
    this.startPosMBFF = -1;
    this.countMBFF = 0;
    fileChannel.close();
  }

  public RandomAccessFile getRandomAccessFile() {
    return this.raf;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("CachedFileChannel is read-only");
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length)
      throws IOException {
    throw new UnsupportedOperationException("CachedFileChannel is read-only");
  }

  @Override
  public FileChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("CachedFileChannel is read-only");
  }

  @Override
  public void force(boolean metaData) throws IOException {
    throw new UnsupportedOperationException("CachedFileChannel is read-only");
  }

  @Override
  public long transferTo(long position, long count, WritableByteChannel target)
      throws IOException {
    throw new UnsupportedOperationException(
        "CachedFileChannel does not support this operation");
  }

  @Override
  public long transferFrom(ReadableByteChannel src, long position, long count)
      throws IOException {
    throw new UnsupportedOperationException("CachedFileChannel is read-only");
  }

  @Override
  public int write(ByteBuffer src, long position) throws IOException {
    throw new UnsupportedOperationException("CachedFileChannel is read-only");
  }

  @Override
  public MappedByteBuffer map(MapMode mode, long position, long size)
      throws IOException {
    throw new UnsupportedOperationException(
        "CachedFileChannel does not support this operation");
  }

  @Override
  public FileLock lock(long position, long size, boolean shared)
      throws IOException {
    throw new UnsupportedOperationException(
        "CachedFileChannel does not support this operation");
  }

  @Override
  public FileLock tryLock(long position, long size, boolean shared)
      throws IOException {
    throw new UnsupportedOperationException(
        "CachedFileChannel does not support this operation");
  }
}
