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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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

    /**
     * Copy the state from the next one into this instance (the previous state
     * placeholder). Used to save the previous state when we are advancing the
     * seeker to the next key/value.
     */
    protected void copyFromNext(SeekerState nextState) {
      if (keyBuffer.length != nextState.keyBuffer.length) {
        keyBuffer = nextState.keyBuffer.clone();
      } else if (!isValid()) {
        // Note: we can only call isValid before we override our state, so this
        // comes before all the assignments at the end of this method.
        System.arraycopy(nextState.keyBuffer, 0, keyBuffer, 0,
             nextState.keyLength);
      } else {
        // don't copy the common prefix between this key and the previous one
        System.arraycopy(nextState.keyBuffer, nextState.lastCommonPrefix,
            keyBuffer, nextState.lastCommonPrefix, nextState.keyLength
                - nextState.lastCommonPrefix);
      }

      valueOffset = nextState.valueOffset;
      keyLength = nextState.keyLength;
      valueLength = nextState.valueLength;
      lastCommonPrefix = nextState.lastCommonPrefix;
      nextKvOffset = nextState.nextKvOffset;
      memstoreTS = nextState.memstoreTS;
    }

  }

  protected abstract static class
      BufferedEncodedSeeker<STATE extends SeekerState>
      implements EncodedSeeker {

    protected final RawComparator<byte[]> comparator;
    protected final SamePrefixComparator<byte[]> samePrefixComparator;
    protected ByteBuffer currentBuffer;
    protected STATE current = createSeekerState(); // always valid
    protected STATE previous = createSeekerState(); // may not be valid

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
    public ByteBuffer getKeyDeepCopy() {
      ByteBuffer keyBuffer = ByteBuffer.allocate(current.keyLength);
      keyBuffer.put(current.keyBuffer, 0, current.keyLength);
      return keyBuffer;
    }

    @Override
    public ByteBuffer getValueShallowCopy() {
      return ByteBuffer.wrap(currentBuffer.array(),
          currentBuffer.arrayOffset() + current.valueOffset,
          current.valueLength);
    }

    @Override
    public ByteBuffer getKeyValueBuffer() {
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
    public KeyValue getKeyValue() {
      ByteBuffer kvBuf = getKeyValueBuffer();
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
    public int seekToKeyInBlock(byte[] key, int offset, int length,
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

          comp = samePrefixComparator.compareIgnoringPrefix(commonPrefix, key,
              offset, length, current.keyBuffer, 0, current.keyLength);
        } else {
          comp = comparator.compare(key, offset, length,
              current.keyBuffer, 0, current.keyLength);
        }

        if (comp == 0) { // exact match
          if (seekBefore) {
            if (!previous.isValid()) {
              // The caller (seekBefore) has to ensure that we are not at the
              // first key in the block.
              throw new IllegalStateException("Cannot seekBefore if " +
                  "positioned at the first key in the block: key=" +
                  Bytes.toStringBinary(key, offset, length));
            }
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
          previous.copyFromNext(current);
          decodeNext();
        } else {
          break;
        }
      } while (true);

      // we hit the end of the block, not an exact match
      return 1;
    }

    private void moveToPrevious() {
      if (!previous.isValid()) {
        throw new IllegalStateException(
            "Can move back only once and not in first key in the block.");
      }

      STATE tmp = previous;
      previous = current;
      current = tmp;

      // move after last key value
      currentBuffer.position(current.nextKvOffset);

      previous.invalidate();
    }

    @SuppressWarnings("unchecked")
    protected STATE createSeekerState() {
      // This will fail for non-default seeker state if the subclass does not
      // override this method.
      return (STATE) new SeekerState();
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
