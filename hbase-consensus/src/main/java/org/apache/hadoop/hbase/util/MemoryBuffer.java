package org.apache.hadoop.hbase.util;

import java.nio.ByteBuffer;

/**
 * Wraps the memory allocated by the Arena. It provides the abstraction of a
 * ByteBuffer to the outside world.
 */
public class MemoryBuffer {

  private final long offset;
  private final int size;
  private final ByteBuffer buffer;
  public static final int UNDEFINED_OFFSET = -1;

  public MemoryBuffer(final ByteBuffer buffer, long offset, int size) {
    this.offset = offset;
    this.size = size;
    this.buffer = buffer;
  }

  public MemoryBuffer(ByteBuffer buffer) {
    this.offset = UNDEFINED_OFFSET;
    this.size = buffer.remaining();
    this.buffer = buffer;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public long getOffset() {
    return offset;
  }

  public int getSize() {
    return size;
  }

  public void flip() {
    if (offset != UNDEFINED_OFFSET) {
      buffer.limit(buffer.position());
      buffer.position((int)offset);
    } else {
      buffer.flip();
    }
  }
}
