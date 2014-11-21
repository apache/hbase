package org.apache.hadoop.hbase.consensus.log;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Arena;
import org.apache.hadoop.hbase.util.MemoryBuffer;
import org.apache.hadoop.util.PureJavaCrc32;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * LogReader provides interfaces to perform read operations against a log file.
 * This class is not thread safe.
 *
 * If multiple threads need to read from the same file, each thread should have
 * its own LogReader object.
 *
 * Each LogReader shall be initialized first before reading. It maintains the
 * reader position internally. The client is able to seek to a particular
 * offset or index, and then start to read the transaction since there.
 *
 * Here is the log file format:
 *
 * ------------------
 * | File Header:   |
 * |  Version    4B |
 * |  Term       8B |
 * |  Index      8B |
 * ------------------
 * | Entry # N      |
 * |  Entry Header  |
 * |   Size     4B  |
 * |   CRC      8B  |
 * |   Index    8B  |
 * |  Entry PayLoad |
 * ------------------
 * | Entry # N      |
 * |  Entry Header  |
 * |   Size     4B  |
 * |   CRC      8B  |
 * |   Index    8B  |
 * |  Entry PayLoad |
 * ----------------
 *     .....
 * ------------------
 * | Entry # N      |
 * |  Entry Header  |
 * |   Size     4B  |
 * |   CRC      8B  |
 * |   Index    8B  |
 * |  Entry PayLoad |
 * ------------------
 */
@NotThreadSafe
public class LogReader {

  public static final Log LOG = LogFactory.getLog(LogReader.class);
  private final File file;

  // File header meta data
  private int version = HConstants.UNDEFINED_VERSION;
  private long currentTerm = HConstants.UNDEFINED_TERM_INDEX;
  private long initialIndex = HConstants.UNDEFINED_TERM_INDEX;

  // The invariant for the current position of the file
  private int curPayLoadSize = HConstants.UNDEFINED_PAYLOAD_SIZE;
  private long curIndex = HConstants.UNDEFINED_TERM_INDEX;
  private long curPayLoadOffset = HConstants.UNDEFINED_PAYLOAD_SIZE;
  private long curCRC = 0;

  //TODO: check if we can share the headBuffer across multiple readers
  private ByteBuffer headBuffer =
    ByteBuffer.allocateDirect(HConstants.RAFT_TXN_HEADER_SIZE);

  /** The CRC instance to compute CRC-32 of a log entry payload */
  private PureJavaCrc32 crc32 = new PureJavaCrc32();
  CachedFileChannel data;

  public LogReader (File file) throws IOException {
    this.file = file;
    data = new CachedFileChannel(new RandomAccessFile(this.file, "r"),
        TransactionLogManager.getLogPrefetchSize());
  }

  public void initialize()throws IOException {
    readFileHeader();
  }

  public File getFile() {
    return file;
  }

  public RandomAccessFile getRandomAccessFile() {
    return data.getRandomAccessFile();
  }

  public void initialize(int version, long currentTerm, long initialIndex) {
    this.version = version;
    this.currentTerm = currentTerm;
    this.initialIndex = initialIndex;
  }

  /**
   * Seek to a particular index and read the transactions from the entry payload
   * @param index
   * @param arena Arena
   * @return transactions of this index
   * @throws IOException if there is no such index in the log
   */
  public MemoryBuffer seekAndRead(long index, final Arena arena) throws IOException {
    long onDiskIndex = HConstants.UNDEFINED_TERM_INDEX;
    if (this.curIndex > index) {
      resetPosition();
    }

    // Read the transaction header and try to match the index
    while (hasMore()) {
      if (curIndex == index) {
        break;
      }

      // Read the transaction header
      readEntryHeader();

      // Compare the index
      if (curIndex != index) {
        // Skip the payload for the non-match
        skipEntryPayload();
      }
    }

    // Throw the exception if there is no matched index.
    if (curIndex != index) {
      resetPosition();
      throw new IOException("There is no expected index: " + index + " in " +
        "this log. On disk index " + onDiskIndex + " " + this.getInitialIndex() +
      this.getCurrentTerm() + " current Index" + curIndex);
    }

    // Return the transactions
    return readEntryPayLoad(arena);
  }

  /**
   * Seek to a particular index, and sanity check the index,
   * and then read the transactions from the entry payload
   * @param offset
   * @param expectedIndex
   * @return transactions of this index
   * @throws IOException if the on disk index does not match with the expected index
   */
  public MemoryBuffer seekAndRead(long offset, long expectedIndex,
                                  final Arena arena) throws IOException {
    // Set the file position
    data.position(offset);

    // Read the transaction header and verify it
    long onDiskIndex = this.readEntryHeader();
    if (onDiskIndex != expectedIndex) {
      // Reset the position
      resetPosition();

      // Throw out the exceptions
      throw new IOException("The expected index " + expectedIndex + " does not match with "
      + " on disk index " + onDiskIndex + " at the offset " + offset);
    }

    // Return the payload
    return readEntryPayLoad(arena);
  }

  /**
   * Whether there are more transactions in the file
   * @return true if there are more transactions in the file
   * @throws IOException
   */
  public boolean hasMore() throws IOException {
    if (data.size() > data.position()) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns the next index from the current position.
   *
   * This assumes that the current position is at the start of the payload
   * of the current index.
   *
   * @return next index
   * @throws IOException
   */
  long next() throws IOException {
    long onDiskIndex = HConstants.UNDEFINED_TERM_INDEX;
    long lastIndex = curIndex;

    // Skip to the next entry
    if (curIndex != HConstants.UNDEFINED_TERM_INDEX) {
      try {
        skipEntryPayload();
      } catch (IOException e) {
        LOG.error("Unable to move to the next entry in the file. The data might" +
          "be corrupted. Current Index :" + curIndex + ", Current Payload " +
          "offset :" + curPayLoadOffset, e);

        // We were unable to read the current entry, which means that it might
        // be corrupted. Reset the position to the start of the entry.
        data.position(
          curPayLoadOffset - HConstants.RAFT_TXN_HEADER_SIZE);
        return HConstants.UNDEFINED_TERM_INDEX;
      }
    }

    // Are there more entries ??
    if (hasMore()) {
      // Read the transaction header
      onDiskIndex = this.readEntryHeader();
    }

    // Verify the next entry. All the entries must be continuous in the log
    if ((lastIndex != HConstants.UNDEFINED_TERM_INDEX) &&
      (lastIndex + 1 != onDiskIndex)) {
        LOG.warn("Next index: " + onDiskIndex + " is not continuous. Previous " +
          "index: " + lastIndex + ", term " + currentTerm
          + ". File: " + (getFile() != null ? getFile().getAbsolutePath() : "?")
          );

      // If we had more data but not contiguous, reset the position to the
      // start of the unknown index
      if (onDiskIndex != HConstants.UNDEFINED_TERM_INDEX) {
        data.position(getCurrentIndexFileOffset());
      }
      return HConstants.UNDEFINED_TERM_INDEX;
    }

    // Return the transactions
    return onDiskIndex;
  }

  /**
   * Get the start offset of the current index in the file.
   *
   * This assumes that the current position is at the start of the payload of
   * the current index.
   *
   * @return offset of the current index
   * @throws IOException
   */
  long getCurrentIndexFileOffset() throws IOException {
    return data.position() - HConstants.RAFT_FILE_HEADER_SIZE;
  }

  /**
   * Close the reader
   * @throws IOException
   */
  public void close() throws IOException {
    this.data.close();
  }

  /**
   * @return the version of the log file
   */
  public int getVersion() {
    return version;
  }

  /**
   * @return the term of the log file
   */
  public long getCurrentTerm() {
    return currentTerm;
  }

  /**
   * @return the initial index of the log file
   */
  public long getInitialIndex() {
    return initialIndex;
  }

  public long getCurrentPosition() throws IOException {
    return data.position();
  }

  public void resetPosition() throws IOException {
    this.data.position(HConstants.RAFT_FILE_HEADER_SIZE);
    this.curIndex = HConstants.UNDEFINED_TERM_INDEX;
    this.curPayLoadSize = HConstants.UNDEFINED_PAYLOAD_SIZE;
  }

  private void skipEntryPayload() throws IOException {
    // Get the current position and the target position
    long targetPosition = this.data.position() + this.curPayLoadSize;

    // Set the target position for the channel
    data.position(targetPosition) ;
  }

  /**
   *
   *  readEntryPayLoad
   *
   *  @param  arena  Arena to be used for memory allocation
   */
  private MemoryBuffer readEntryPayLoad(final Arena arena) throws IOException {

    MemoryBuffer buffer = TransactionLogManager.allocateBuffer(arena,
        curPayLoadSize);

    if (curPayLoadOffset != data.position()) {
      data.position(curPayLoadOffset);
    }

    buffer.getBuffer().limit(buffer.getBuffer().position() + curPayLoadSize);
    data.read(buffer.getBuffer());
    buffer.flip();

    if (!verifyCRC(buffer.getBuffer())) {
      long faultyIndex = this.curIndex;
      resetPosition();
      throw new IOException("The CRC for the entry payload of index " +
        faultyIndex + " does not match with the expected CRC read from the " +
        "entry header.");
    }
    return buffer;
  }

  private long readEntryHeader() throws IOException {

    // Read the header of the transaction

    headBuffer.clear();
    data.read(headBuffer);
    headBuffer.flip();

    // Read the payload size
    this.curPayLoadSize = headBuffer.getInt();

    // Read the CRC
    this.curCRC = headBuffer.getLong();

    // Read the current index
    this.curIndex = headBuffer.getLong();

    // Cache the offset
    this.curPayLoadOffset = data.position();

    // Sanity check the payload size
    assert this.curPayLoadSize <= this.data.size() - this.data.position();

    // Return the OnDisk Index
    return curIndex;
  }

  private void readFileHeader() throws IOException {
    // Set the position to 0
    this.data.position(0);

    // Read the FileHeader to buffer
    headBuffer.clear();
    this.data.read(headBuffer);
    headBuffer.flip();

    if (headBuffer.remaining() != HConstants.RAFT_FILE_HEADER_SIZE) {
      throw new IOException("Cannot read the header of the file " + file);
    }
    this.version = headBuffer.getInt();
    this.currentTerm = headBuffer.getLong();
    this.initialIndex = headBuffer.getLong();
  }

  /**
   * Verify the CRC for the payload buffer and return the result
   * @param payloadBuffer
   * @return true if the CRC of the payload buffer is equal to the curCRC from the header
   */
  private boolean verifyCRC(ByteBuffer payloadBuffer) {
    crc32.reset();
    crc32.update(payloadBuffer.array(),
      payloadBuffer.position() + payloadBuffer.arrayOffset(),
      payloadBuffer.remaining());

    return crc32.getValue() == this.curCRC;
  }
}
