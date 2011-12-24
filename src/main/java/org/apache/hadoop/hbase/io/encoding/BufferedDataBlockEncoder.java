/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.encoding;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SamePrefixComparator;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * Base class for all data block encoders that use a buffer.
 */
abstract class BufferedDataBlockEncoder implements DataBlockEncoder {

  private static int INITIAL_KEY_BUFFER_SIZE = 512;

  @Override
  public ByteBuffer uncompressKeyValues(DataInputStream source,
      boolean includesMemstoreTS) throws IOException {
    return uncompressKeyValues(source, 0, 0, includesMemstoreTS);
  }

  protected static class SeekerState {
    protected int valueOffset = -1;
    protected int keyLength;
    protected int valueLength;
    protected int lastCommonPrefix;

    /** We need to store a copy of the key. */
    protected byte[] keyBuffer = new byte[INITIAL_KEY_BUFFER_SIZE];

    protected long memstoreTS;
    protected int nextKvOffset;

    protected boolean isValid() {
      return valueOffset != -1;
    }

    protected void invalidate() {
      valueOffset = -1;
    }

    protected void ensureSpaceForKey() {
      if (keyLength > keyBuffer.length) {
        // rare case, but we need to handle arbitrary length of key
        int newKeyBufferLength = Math.max(keyBuffer.length, 1) * 2;
        while (keyLength > newKeyBufferLength) {
          newKeyBufferLength *= 2;
        }
        byte[] newKeyBuffer = new byte[newKeyBufferLength];
        System.arraycopy(keyBuffer, 0, newKeyBuffer, 0, keyBuffer.length);
        keyBuffer = newKeyBuffer;
      }
    }
  }

  protected abstract static class BufferedEncodedSeeker
      implements EncodedSeeker {

    protected final RawComparator<byte[]> comparator;
    protected final SamePrefixComparator<byte[]> samePrefixComparator;
    protected ByteBuffer currentBuffer;
    protected SeekerState current = new SeekerState(); // always valid
    protected SeekerState previous = new SeekerState(); // may not be valid

    @SuppressWarnings("unchecked")
    public BufferedEncodedSeeker(RawComparator<byte[]> comparator) {
      this.comparator = comparator;
      if (comparator instanceof SamePrefixComparator) {
        this.samePrefixComparator = (SamePrefixComparator<byte[]>) comparator;
      } else {
        this.samePrefixComparator = null;
      }
    }

    @Override
    public void setCurrentBuffer(ByteBuffer buffer) {
      currentBuffer = buffer;
      decodeFirst();
      previous.invalidate();
    }

    @Override
    public ByteBuffer getKey() {
      ByteBuffer keyBuffer = ByteBuffer.allocate(current.keyLength);
      keyBuffer.put(current.keyBuffer, 0, current.keyLength);
      return keyBuffer;
    }

    @Override
    public ByteBuffer getValue() {
      return ByteBuffer.wrap(currentBuffer.array(),
          currentBuffer.arrayOffset() + current.valueOffset,
          current.valueLength);
    }

    @Override
    public ByteBuffer getKeyValue() {
      ByteBuffer kvBuffer = ByteBuffer.allocate(
          2 * Bytes.SIZEOF_INT + current.keyLength + current.valueLength);
      kvBuffer.putInt(current.keyLength);
      kvBuffer.putInt(current.valueLength);
      kvBuffer.put(current.keyBuffer, 0, current.keyLength);
      kvBuffer.put(currentBuffer.array(),
          currentBuffer.arrayOffset() + current.valueOffset,
          current.valueLength);
      return kvBuffer;
    }

    @Override
    public KeyValue getKeyValueObject() {
      ByteBuffer kvBuf = getKeyValue();
      KeyValue kv = new KeyValue(kvBuf.array(), kvBuf.arrayOffset());
      kv.setMemstoreTS(current.memstoreTS);
      return kv;
    }

    @Override
    public void rewind() {
      currentBuffer.rewind();
      decodeFirst();
      previous.invalidate();
    }

    @Override
    public boolean next() {
      if (!currentBuffer.hasRemaining()) {
        return false;
      }
      decodeNext();
      previous.invalidate();
      return true;
    }

    @Override
    public int blockSeekTo(byte[] key, int offset, int length,
        boolean seekBefore) {
      int commonPrefix = 0;
      previous.invalidate();
      do {
        int comp;
        if (samePrefixComparator != null) {
          commonPrefix = Math.min(commonPrefix, current.lastCommonPrefix);

          // extend commonPrefix
          commonPrefix += ByteBufferUtils.findCommonPrefix(
              key, offset + commonPrefix, length - commonPrefix,
              current.keyBuffer, commonPrefix,
              current.keyLength - commonPrefix);

          comp = samePrefixComparator.compareIgnoringPrefix(
              commonPrefix,
              key, offset, length, current.keyBuffer, 0, current.keyLength);
        } else {
          comp = comparator.compare(key, offset, length,
              current.keyBuffer, 0, current.keyLength);
        }

        if (comp == 0) { // exact match
          if (seekBefore) {
            moveToPrevious();
            return 1;
          }
          return 0;
        }

        if (comp < 0) { // already too large, check previous
          if (previous.isValid()) {
            moveToPrevious();
          }
          return 1;
        }

        // move to next, if more data is available
        if (currentBuffer.hasRemaining()) {
          savePrevious();
          decodeNext();
        } else {
          break;
        }
      } while (true);

      // we hit end of file, not exact match
      return 1;
    }

    private void moveToPrevious() {
      if (!previous.isValid()) {
        throw new IllegalStateException(
            "Can move back only once and not in first key in the block.");
      }

      SeekerState tmp = previous;
      previous = current;
      current = tmp;

      // move after last key value
      currentBuffer.position(current.nextKvOffset);

      previous.invalidate();
    }

    private void savePrevious() {
      previous.valueOffset = current.valueOffset;
      previous.keyLength = current.keyLength;
      previous.valueLength = current.valueLength;
      previous.lastCommonPrefix = current.lastCommonPrefix;
      previous.nextKvOffset = current.nextKvOffset;
      if (previous.keyBuffer.length != current.keyBuffer.length) {
        previous.keyBuffer = current.keyBuffer.clone();
      } else if (!previous.isValid()) {
        System.arraycopy(current.keyBuffer, 0, previous.keyBuffer, 0,
             current.keyLength);
      } else {
        // don't copy the common prefix between this key and the previous one
        System.arraycopy(current.keyBuffer, current.lastCommonPrefix,
            previous.keyBuffer, current.lastCommonPrefix, current.keyLength
                - current.lastCommonPrefix);
      }
    }

    abstract protected void decodeFirst();
    abstract protected void decodeNext();
  }

  protected final void afterEncodingKeyValue(ByteBuffer in,
      DataOutputStream out, boolean includesMemstoreTS) {
    if (includesMemstoreTS) {
      // Copy memstore timestamp from the byte buffer to the output stream.
      long memstoreTS = -1;
      try {
        memstoreTS = ByteBufferUtils.readVLong(in);
        WritableUtils.writeVLong(out, memstoreTS);
      } catch (IOException ex) {
        throw new RuntimeException("Unable to copy memstore timestamp " +
            memstoreTS + " after encoding a key/value");
      }
    }
  }

  static DataInput asDataInputStream(final ByteBuffer in) {
    return new DataInputStream(new InputStream() {
      @Override
      public int read() throws IOException {
        return in.get() & 0xff;
      }
    });
  }

  protected final void afterDecodingKeyValue(DataInputStream source,
      ByteBuffer dest, boolean includesMemstoreTS) {
    if (includesMemstoreTS) {
      long memstoreTS = -1;
      try {
        // Copy memstore timestamp from the data input stream to the byte
        // buffer.
        memstoreTS = WritableUtils.readVLong(source);
        ByteBufferUtils.writeVLong(dest, memstoreTS);
      } catch (IOException ex) {
        throw new RuntimeException("Unable to copy memstore timestamp " +
            memstoreTS + " after decoding a key/value");
      }
    }
  }

}
