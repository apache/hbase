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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.SamePrefixComparator;
import org.apache.hadoop.hbase.io.TagCompressionContext;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

/**
 * Base class for all data block encoders that use a buffer.
 */
@InterfaceAudience.Private
abstract class BufferedDataBlockEncoder implements DataBlockEncoder {

  private static int INITIAL_KEY_BUFFER_SIZE = 512;

  @Override
  public ByteBuffer decodeKeyValues(DataInputStream source,
      HFileBlockDecodingContext blkDecodingCtx) throws IOException {
    if (blkDecodingCtx.getClass() != HFileBlockDefaultDecodingContext.class) {
      throw new IOException(this.getClass().getName() + " only accepts "
          + HFileBlockDefaultDecodingContext.class.getName() + " as the decoding context.");
    }

    HFileBlockDefaultDecodingContext decodingCtx =
        (HFileBlockDefaultDecodingContext) blkDecodingCtx;
    if (decodingCtx.getHFileContext().isIncludesTags()
        && decodingCtx.getHFileContext().isCompressTags()) {
      if (decodingCtx.getTagCompressionContext() != null) {
        // It will be overhead to create the TagCompressionContext again and again for every block
        // decoding.
        decodingCtx.getTagCompressionContext().clear();
      } else {
        try {
          TagCompressionContext tagCompressionContext = new TagCompressionContext(
              LRUDictionary.class, Byte.MAX_VALUE);
          decodingCtx.setTagCompressionContext(tagCompressionContext);
        } catch (Exception e) {
          throw new IOException("Failed to initialize TagCompressionContext", e);
        }
      }
    }
    return internalDecodeKeyValues(source, 0, 0, decodingCtx);
  }

  protected static class SeekerState {
    protected int valueOffset = -1;
    protected int keyLength;
    protected int valueLength;
    protected int lastCommonPrefix;
    protected int tagsLength = 0;
    protected int tagsOffset = -1;
    protected int tagsCompressedLength = 0;
    protected boolean uncompressTags = true;

    /** We need to store a copy of the key. */
    protected byte[] keyBuffer = new byte[INITIAL_KEY_BUFFER_SIZE];
    protected byte[] tagsBuffer = new byte[INITIAL_KEY_BUFFER_SIZE];

    protected long memstoreTS;
    protected int nextKvOffset;

    protected boolean isValid() {
      return valueOffset != -1;
    }

    protected void invalidate() {
      valueOffset = -1;
      tagsCompressedLength = 0;
      uncompressTags = true;
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

    protected void ensureSpaceForTags() {
      if (tagsLength > tagsBuffer.length) {
        // rare case, but we need to handle arbitrary length of tags
        int newTagsBufferLength = Math.max(tagsBuffer.length, 1) * 2;
        while (tagsLength > newTagsBufferLength) {
          newTagsBufferLength *= 2;
        }
        byte[] newTagsBuffer = new byte[newTagsBufferLength];
        System.arraycopy(tagsBuffer, 0, newTagsBuffer, 0, tagsBuffer.length);
        tagsBuffer = newTagsBuffer;
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
    protected HFileBlockDecodingContext decodingCtx;
    protected final KVComparator comparator;
    protected final SamePrefixComparator<byte[]> samePrefixComparator;
    protected ByteBuffer currentBuffer;
    protected STATE current = createSeekerState(); // always valid
    protected STATE previous = createSeekerState(); // may not be valid
    protected TagCompressionContext tagCompressionContext = null;

    public BufferedEncodedSeeker(KVComparator comparator,
        HFileBlockDecodingContext decodingCtx) {
      this.comparator = comparator;
      this.samePrefixComparator = comparator;
      this.decodingCtx = decodingCtx;
      if (decodingCtx.getHFileContext().isCompressTags()) {
        try {
          tagCompressionContext = new TagCompressionContext(LRUDictionary.class, Byte.MAX_VALUE);
        } catch (Exception e) {
          throw new RuntimeException("Failed to initialize TagCompressionContext", e);
        }
      }
    }
    
    protected boolean includesMvcc() {
      return this.decodingCtx.getHFileContext().isIncludesMvcc();
    }

    protected boolean includesTags() {
      return this.decodingCtx.getHFileContext().isIncludesTags();
    }

    @Override
    public int compareKey(KVComparator comparator, byte[] key, int offset, int length) {
      return comparator.compareFlatKey(key, offset, length,
          current.keyBuffer, 0, current.keyLength);
    }

    @Override
    public void setCurrentBuffer(ByteBuffer buffer) {
      if (this.tagCompressionContext != null) {
        this.tagCompressionContext.clear();
      }
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
      ByteBuffer kvBuffer = createKVBuffer();
      kvBuffer.putInt(current.keyLength);
      kvBuffer.putInt(current.valueLength);
      kvBuffer.put(current.keyBuffer, 0, current.keyLength);
      kvBuffer.put(currentBuffer.array(),
          currentBuffer.arrayOffset() + current.valueOffset,
          current.valueLength);
      if (current.tagsLength > 0) {
        // Put short as unsigned
        kvBuffer.put((byte)(current.tagsLength >> 8 & 0xff));
        kvBuffer.put((byte)(current.tagsLength & 0xff));
        if (current.tagsOffset != -1) {
          // the offset of the tags bytes in the underlying buffer is marked. So the temp
          // buffer,tagsBuffer was not been used.
          kvBuffer.put(currentBuffer.array(), currentBuffer.arrayOffset() + current.tagsOffset,
              current.tagsLength);
        } else {
          // When tagsOffset is marked as -1, tag compression was present and so the tags were
          // uncompressed into temp buffer, tagsBuffer. Let us copy it from there
          kvBuffer.put(current.tagsBuffer, 0, current.tagsLength);
        }
      }
      return kvBuffer;
    }

    protected ByteBuffer createKVBuffer() {
      int kvBufSize = (int) KeyValue.getKeyValueDataStructureSize(current.keyLength,
          current.valueLength, current.tagsLength);
      ByteBuffer kvBuffer = ByteBuffer.allocate(kvBufSize);
      return kvBuffer;
    }

    @Override
    public KeyValue getKeyValue() {
      ByteBuffer kvBuf = getKeyValueBuffer();
      KeyValue kv = new KeyValue(kvBuf.array(), kvBuf.arrayOffset(), kvBuf.array().length
          - kvBuf.arrayOffset());
      kv.setMvccVersion(current.memstoreTS);
      return kv;
    }

    @Override
    public void rewind() {
      currentBuffer.rewind();
      if (tagCompressionContext != null) {
        tagCompressionContext.clear();
      }
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

    protected void decodeTags() {
      current.tagsLength = ByteBufferUtils.readCompressedInt(currentBuffer);
      if (tagCompressionContext != null) {
        if (current.uncompressTags) {
          // Tag compression is been used. uncompress it into tagsBuffer
          current.ensureSpaceForTags();
          try {
            current.tagsCompressedLength = tagCompressionContext.uncompressTags(currentBuffer,
                current.tagsBuffer, 0, current.tagsLength);
          } catch (IOException e) {
            throw new RuntimeException("Exception while uncompressing tags", e);
          }
        } else {
          ByteBufferUtils.skip(currentBuffer, current.tagsCompressedLength);
          current.uncompressTags = true;// Reset this.
        }
        current.tagsOffset = -1;
      } else {
        // When tag compress is not used, let us not do temp copying of tags bytes into tagsBuffer.
        // Just mark the tags Offset so as to create the KV buffer later in getKeyValueBuffer()
        current.tagsOffset = currentBuffer.position();
        ByteBufferUtils.skip(currentBuffer, current.tagsLength);
      }
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
          comp = comparator.compareFlatKey(key, offset, length,
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
          } else {
            return HConstants.INDEX_KEY_MAGIC; // using optimized index key
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
      // Already decoded the tag bytes. We cache this tags into current state and also the total
      // compressed length of the tags bytes. For the next time decodeNext() we don't need to decode
      // the tags again. This might pollute the Data Dictionary what we use for the compression.
      // When current.uncompressTags is false, we will just reuse the current.tagsBuffer and skip
      // 'tagsCompressedLength' bytes of source stream.
      // See in decodeTags()
      current.tagsBuffer = previous.tagsBuffer;
      current.tagsCompressedLength = previous.tagsCompressedLength;
      current.uncompressTags = false;
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
      DataOutputStream out, HFileBlockDefaultEncodingContext encodingCtx) throws IOException {
    if (encodingCtx.getHFileContext().isIncludesTags()) {
      // Read short as unsigned, high byte first
      int tagsLength = ((in.get() & 0xff) << 8) ^ (in.get() & 0xff);
      ByteBufferUtils.putCompressedInt(out, tagsLength);
      // There are some tags to be written
      if (tagsLength > 0) {
        TagCompressionContext tagCompressionContext = encodingCtx.getTagCompressionContext();
        // When tag compression is enabled, tagCompressionContext will have a not null value. Write
        // the tags using Dictionary compression in such a case
        if (tagCompressionContext != null) {
          tagCompressionContext.compressTags(out, in, tagsLength);
        } else {
          ByteBufferUtils.moveBufferToStream(out, in, tagsLength);
        }
      }
    }
    if (encodingCtx.getHFileContext().isIncludesMvcc()) {
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
      ByteBuffer dest, HFileBlockDefaultDecodingContext decodingCtx) throws IOException {
    if (decodingCtx.getHFileContext().isIncludesTags()) {
      int tagsLength = ByteBufferUtils.readCompressedInt(source);
      // Put as unsigned short
      dest.put((byte)((tagsLength >> 8) & 0xff));
      dest.put((byte)(tagsLength & 0xff));
      if (tagsLength > 0) {
        TagCompressionContext tagCompressionContext = decodingCtx.getTagCompressionContext();
        // When tag compression is been used in this file, tagCompressionContext will have a not
        // null value passed.
        if (tagCompressionContext != null) {
          tagCompressionContext.uncompressTags(source, dest, tagsLength);
        } else {
          ByteBufferUtils.copyFromStreamToBuffer(dest, source, tagsLength);
        }
      }
    }
    if (decodingCtx.getHFileContext().isIncludesMvcc()) {
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

  @Override
  public HFileBlockEncodingContext newDataBlockEncodingContext(DataBlockEncoding encoding,
      byte[] header, HFileContext meta) {
    return new HFileBlockDefaultEncodingContext(encoding, header, meta);
  }

  @Override
  public HFileBlockDecodingContext newDataBlockDecodingContext(HFileContext meta) {
    return new HFileBlockDefaultDecodingContext(meta);
  }

  /**
   * Compress KeyValues and write them to output buffer.
   * @param out Where to write compressed data.
   * @param in Source of KeyValue for compression.
   * @param encodingCtx use the Encoding ctx associated with the current block
   * @throws IOException If there is an error writing to output stream.
   */
  public abstract void internalEncodeKeyValues(DataOutputStream out,
      ByteBuffer in, HFileBlockDefaultEncodingContext encodingCtx) throws IOException;

  protected abstract ByteBuffer internalDecodeKeyValues(DataInputStream source,
      int allocateHeaderLength, int skipLastBytes, HFileBlockDefaultDecodingContext decodingCtx)
      throws IOException;

  @Override
  public void encodeKeyValues(ByteBuffer in,
      HFileBlockEncodingContext blkEncodingCtx) throws IOException {
    if (blkEncodingCtx.getClass() != HFileBlockDefaultEncodingContext.class) {
      throw new IOException (this.getClass().getName() + " only accepts "
          + HFileBlockDefaultEncodingContext.class.getName() + " as the " +
          "encoding context.");
    }

    HFileBlockDefaultEncodingContext encodingCtx =
        (HFileBlockDefaultEncodingContext) blkEncodingCtx;
    encodingCtx.prepareEncoding();
    DataOutputStream dataOut = encodingCtx.getOutputStreamForEncoder();
    if (encodingCtx.getHFileContext().isIncludesTags()
        && encodingCtx.getHFileContext().isCompressTags()) {
      if (encodingCtx.getTagCompressionContext() != null) {
        // It will be overhead to create the TagCompressionContext again and again for every block
        // encoding.
        encodingCtx.getTagCompressionContext().clear();
      } else {
        try {
          TagCompressionContext tagCompressionContext = new TagCompressionContext(
              LRUDictionary.class, Byte.MAX_VALUE);
          encodingCtx.setTagCompressionContext(tagCompressionContext);
        } catch (Exception e) {
          throw new IOException("Failed to initialize TagCompressionContext", e);
        }
      }
    }
    internalEncodeKeyValues(dataOut, in, encodingCtx);
    if (encodingCtx.getDataBlockEncoding() != DataBlockEncoding.NONE) {
      encodingCtx.postEncoding(BlockType.ENCODED_DATA);
    } else {
      encodingCtx.postEncoding(BlockType.DATA);
    }
  }

  /**
   * Asserts that there is at least the given amount of unfilled space
   * remaining in the given buffer.
   * @param out typically, the buffer we are writing to
   * @param length the required space in the buffer
   * @throws EncoderBufferTooSmallException If there are no enough bytes.
   */
  protected static void ensureSpace(ByteBuffer out, int length)
      throws EncoderBufferTooSmallException {
    if (out.position() + length > out.limit()) {
      throw new EncoderBufferTooSmallException(
          "Buffer position=" + out.position() +
          ", buffer limit=" + out.limit() +
          ", length to be written=" + length);
    }
  }

}
