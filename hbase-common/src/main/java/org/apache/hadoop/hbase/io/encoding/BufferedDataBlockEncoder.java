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
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.ByteBufferedCell;
import org.apache.hadoop.hbase.ByteBufferedKeyOnlyKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.SettableSequenceId;
import org.apache.hadoop.hbase.Streamable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.TagCompressionContext;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.apache.hadoop.io.WritableUtils;

/**
 * Base class for all data block encoders that use a buffer.
 */
@InterfaceAudience.Private
abstract class BufferedDataBlockEncoder implements DataBlockEncoder {
  /**
   * TODO: This datablockencoder is dealing in internals of hfileblocks. Purge reference to HFBs
   */
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

  /********************* common prefixes *************************/
  // Having this as static is fine but if META is having DBE then we should
  // change this.
  public static int compareCommonRowPrefix(Cell left, Cell right, int rowCommonPrefix) {
    return Bytes.compareTo(left.getRowArray(), left.getRowOffset() + rowCommonPrefix,
        left.getRowLength() - rowCommonPrefix, right.getRowArray(), right.getRowOffset()
            + rowCommonPrefix, right.getRowLength() - rowCommonPrefix);
  }

  public static int compareCommonFamilyPrefix(Cell left, Cell right, int familyCommonPrefix) {
    return Bytes.compareTo(left.getFamilyArray(), left.getFamilyOffset() + familyCommonPrefix,
        left.getFamilyLength() - familyCommonPrefix, right.getFamilyArray(),
        right.getFamilyOffset() + familyCommonPrefix, right.getFamilyLength() - familyCommonPrefix);
  }

  public static int compareCommonQualifierPrefix(Cell left, Cell right, int qualCommonPrefix) {
    return Bytes.compareTo(left.getQualifierArray(), left.getQualifierOffset() + qualCommonPrefix,
        left.getQualifierLength() - qualCommonPrefix, right.getQualifierArray(),
        right.getQualifierOffset() + qualCommonPrefix, right.getQualifierLength()
            - qualCommonPrefix);
  }

  protected static class SeekerState {
    protected ByteBuff currentBuffer;
    protected TagCompressionContext tagCompressionContext;
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
    protected KeyValue.KeyOnlyKeyValue currentKey = new KeyValue.KeyOnlyKeyValue();
    // A temp pair object which will be reused by ByteBuff#asSubByteBuffer calls. This avoids too
    // many object creations.
    private final ObjectIntPair<ByteBuffer> tmpPair;
    private final boolean includeTags;

    public SeekerState(ObjectIntPair<ByteBuffer> tmpPair, boolean includeTags) {
      this.tmpPair = tmpPair;
      this.includeTags = includeTags;
    }

    protected boolean isValid() {
      return valueOffset != -1;
    }

    protected void invalidate() {
      valueOffset = -1;
      tagsCompressedLength = 0;
      currentKey = new KeyValue.KeyOnlyKeyValue();
      uncompressTags = true;
      currentBuffer = null;
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

    protected void setKey(byte[] keyBuffer, long memTS) {
      currentKey.setKey(keyBuffer, 0, keyLength);
      memstoreTS = memTS;
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
      currentKey = nextState.currentKey;

      valueOffset = nextState.valueOffset;
      keyLength = nextState.keyLength;
      valueLength = nextState.valueLength;
      lastCommonPrefix = nextState.lastCommonPrefix;
      nextKvOffset = nextState.nextKvOffset;
      memstoreTS = nextState.memstoreTS;
      currentBuffer = nextState.currentBuffer;
      tagsOffset = nextState.tagsOffset;
      tagsLength = nextState.tagsLength;
      if (nextState.tagCompressionContext != null) {
        tagCompressionContext = nextState.tagCompressionContext;
      }
    }

    public Cell toCell() {
      // Buffer backing the value and tags part from the HFileBlock's buffer
      // When tag compression in use, this will be only the value bytes area.
      ByteBuffer valAndTagsBuffer;
      int vOffset;
      int valAndTagsLength = this.valueLength;
      int tagsLenSerializationSize = 0;
      if (this.includeTags && this.tagCompressionContext == null) {
        // Include the tags part also. This will be the tags bytes + 2 bytes of for storing tags
        // length
        tagsLenSerializationSize = this.tagsOffset - (this.valueOffset + this.valueLength);
        valAndTagsLength += tagsLenSerializationSize + this.tagsLength;
      }
      this.currentBuffer.asSubByteBuffer(this.valueOffset, valAndTagsLength, this.tmpPair);
      valAndTagsBuffer = this.tmpPair.getFirst();
      vOffset = this.tmpPair.getSecond();// This is the offset to value part in the BB
      if (valAndTagsBuffer.hasArray()) {
        return toOnheapCell(valAndTagsBuffer, vOffset, tagsLenSerializationSize);
      } else {
        return toOffheapCell(valAndTagsBuffer, vOffset, tagsLenSerializationSize);
      }
    }

    private Cell toOnheapCell(ByteBuffer valAndTagsBuffer, int vOffset,
        int tagsLenSerializationSize) {
      byte[] tagsArray = HConstants.EMPTY_BYTE_ARRAY;
      int tOffset = 0;
      if (this.includeTags) {
        if (this.tagCompressionContext == null) {
          tagsArray = valAndTagsBuffer.array();
          tOffset = valAndTagsBuffer.arrayOffset() + vOffset + this.valueLength
              + tagsLenSerializationSize;
        } else {
          tagsArray = Bytes.copy(tagsBuffer, 0, this.tagsLength);
          tOffset = 0;
        }
      }
      return new OnheapDecodedCell(Bytes.copy(keyBuffer, 0, this.keyLength),
          currentKey.getRowLength(), currentKey.getFamilyOffset(), currentKey.getFamilyLength(),
          currentKey.getQualifierOffset(), currentKey.getQualifierLength(),
          currentKey.getTimestamp(), currentKey.getTypeByte(), valAndTagsBuffer.array(),
          valAndTagsBuffer.arrayOffset() + vOffset, this.valueLength, memstoreTS, tagsArray,
          tOffset, this.tagsLength);
    }

    private Cell toOffheapCell(ByteBuffer valAndTagsBuffer, int vOffset,
        int tagsLenSerializationSize) {
      ByteBuffer tagsBuf =  HConstants.EMPTY_BYTE_BUFFER;
      int tOffset = 0;
      if (this.includeTags) {
        if (this.tagCompressionContext == null) {
          tagsBuf = valAndTagsBuffer;
          tOffset = vOffset + this.valueLength + tagsLenSerializationSize;
        } else {
          tagsBuf = ByteBuffer.wrap(Bytes.copy(tagsBuffer, 0, this.tagsLength));
          tOffset = 0;
        }
      }
      return new OffheapDecodedCell(ByteBuffer.wrap(Bytes.copy(keyBuffer, 0, this.keyLength)),
          currentKey.getRowLength(), currentKey.getFamilyOffset(), currentKey.getFamilyLength(),
          currentKey.getQualifierOffset(), currentKey.getQualifierLength(),
          currentKey.getTimestamp(), currentKey.getTypeByte(), valAndTagsBuffer, vOffset,
          this.valueLength, memstoreTS, tagsBuf, tOffset, this.tagsLength);
    }
  }

  /**
   * Copies only the key part of the keybuffer by doing a deep copy and passes the
   * seeker state members for taking a clone.
   * Note that the value byte[] part is still pointing to the currentBuffer and
   * represented by the valueOffset and valueLength
   */
  // We return this as a Cell to the upper layers of read flow and might try setting a new SeqId
  // there. So this has to be an instance of SettableSequenceId.
  protected static class OnheapDecodedCell implements Cell, HeapSize, SettableSequenceId,
      Streamable {
    private static final long FIXED_OVERHEAD = ClassSize.align(ClassSize.OBJECT
        + (3 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_LONG) + (7 * Bytes.SIZEOF_INT)
        + (Bytes.SIZEOF_SHORT) + (2 * Bytes.SIZEOF_BYTE) + (3 * ClassSize.ARRAY));
    private byte[] keyOnlyBuffer;
    private short rowLength;
    private int familyOffset;
    private byte familyLength;
    private int qualifierOffset;
    private int qualifierLength;
    private long timestamp;
    private byte typeByte;
    private byte[] valueBuffer;
    private int valueOffset;
    private int valueLength;
    private byte[] tagsBuffer;
    private int tagsOffset;
    private int tagsLength;
    private long seqId;

    protected OnheapDecodedCell(byte[] keyBuffer, short rowLength, int familyOffset,
        byte familyLength, int qualOffset, int qualLength, long timeStamp, byte typeByte,
        byte[] valueBuffer, int valueOffset, int valueLen, long seqId, byte[] tagsBuffer,
        int tagsOffset, int tagsLength) {
      this.keyOnlyBuffer = keyBuffer;
      this.rowLength = rowLength;
      this.familyOffset = familyOffset;
      this.familyLength = familyLength;
      this.qualifierOffset = qualOffset;
      this.qualifierLength = qualLength;
      this.timestamp = timeStamp;
      this.typeByte = typeByte;
      this.valueBuffer = valueBuffer;
      this.valueOffset = valueOffset;
      this.valueLength = valueLen;
      this.tagsBuffer = tagsBuffer;
      this.tagsOffset = tagsOffset;
      this.tagsLength = tagsLength;
      setSequenceId(seqId);
    }

    @Override
    public byte[] getRowArray() {
      return keyOnlyBuffer;
    }

    @Override
    public byte[] getFamilyArray() {
      return keyOnlyBuffer;
    }

    @Override
    public byte[] getQualifierArray() {
      return keyOnlyBuffer;
    }

    @Override
    public int getRowOffset() {
      return Bytes.SIZEOF_SHORT;
    }

    @Override
    public short getRowLength() {
      return rowLength;
    }

    @Override
    public int getFamilyOffset() {
      return familyOffset;
    }

    @Override
    public byte getFamilyLength() {
      return familyLength;
    }

    @Override
    public int getQualifierOffset() {
      return qualifierOffset;
    }

    @Override
    public int getQualifierLength() {
      return qualifierLength;
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public byte getTypeByte() {
      return typeByte;
    }

    @Override
    public long getSequenceId() {
      return seqId;
    }

    @Override
    public byte[] getValueArray() {
      return this.valueBuffer;
    }

    @Override
    public int getValueOffset() {
      return valueOffset;
    }

    @Override
    public int getValueLength() {
      return valueLength;
    }

    @Override
    public byte[] getTagsArray() {
      return this.tagsBuffer;
    }

    @Override
    public int getTagsOffset() {
      return this.tagsOffset;
    }

    @Override
    public int getTagsLength() {
      return tagsLength;
    }

    @Override
    public String toString() {
      return KeyValue.keyToString(this.keyOnlyBuffer, 0, KeyValueUtil.keyLength(this)) + "/vlen="
          + getValueLength() + "/seqid=" + seqId;
    }

    @Override
    public void setSequenceId(long seqId) {
      this.seqId = seqId;
    }

    @Override
    public long heapSize() {
      return FIXED_OVERHEAD + rowLength + familyLength + qualifierLength + valueLength + tagsLength;
    }

    @Override
    public int write(OutputStream out) throws IOException {
      return write(out, true);
    }

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
      int lenToWrite = KeyValueUtil.length(rowLength, familyLength, qualifierLength, valueLength,
          tagsLength, withTags);
      ByteBufferUtils.putInt(out, lenToWrite);
      ByteBufferUtils.putInt(out, keyOnlyBuffer.length);
      ByteBufferUtils.putInt(out, valueLength);
      // Write key
      out.write(keyOnlyBuffer);
      // Write value
      out.write(this.valueBuffer, this.valueOffset, this.valueLength);
      if (withTags) {
        // 2 bytes tags length followed by tags bytes
        // tags length is serialized with 2 bytes only(short way) even if the type is int.
        // As this is non -ve numbers, we save the sign bit. See HBASE-11437
        out.write((byte) (0xff & (this.tagsLength >> 8)));
        out.write((byte) (0xff & this.tagsLength));
        out.write(this.tagsBuffer, this.tagsOffset, this.tagsLength);
      }
      return lenToWrite + Bytes.SIZEOF_INT;
    }
  }

  protected static class OffheapDecodedCell extends ByteBufferedCell implements HeapSize,
      SettableSequenceId, Streamable {
    private static final long FIXED_OVERHEAD = ClassSize.align(ClassSize.OBJECT
        + (3 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_LONG) + (7 * Bytes.SIZEOF_INT)
        + (Bytes.SIZEOF_SHORT) + (2 * Bytes.SIZEOF_BYTE) + (3 * ClassSize.BYTE_BUFFER));
    private ByteBuffer keyBuffer;
    private short rowLength;
    private int familyOffset;
    private byte familyLength;
    private int qualifierOffset;
    private int qualifierLength;
    private long timestamp;
    private byte typeByte;
    private ByteBuffer valueBuffer;
    private int valueOffset;
    private int valueLength;
    private ByteBuffer tagsBuffer;
    private int tagsOffset;
    private int tagsLength;
    private long seqId;

    protected OffheapDecodedCell(ByteBuffer keyBuffer, short rowLength, int familyOffset,
        byte familyLength, int qualOffset, int qualLength, long timeStamp, byte typeByte,
        ByteBuffer valueBuffer, int valueOffset, int valueLen, long seqId, ByteBuffer tagsBuffer,
        int tagsOffset, int tagsLength) {
      // The keyBuffer is always onheap
      assert keyBuffer.hasArray();
      assert keyBuffer.arrayOffset() == 0;
      this.keyBuffer = keyBuffer;
      this.rowLength = rowLength;
      this.familyOffset = familyOffset;
      this.familyLength = familyLength;
      this.qualifierOffset = qualOffset;
      this.qualifierLength = qualLength;
      this.timestamp = timeStamp;
      this.typeByte = typeByte;
      this.valueBuffer = valueBuffer;
      this.valueOffset = valueOffset;
      this.valueLength = valueLen;
      this.tagsBuffer = tagsBuffer;
      this.tagsOffset = tagsOffset;
      this.tagsLength = tagsLength;
      setSequenceId(seqId);
    }

    @Override
    public byte[] getRowArray() {
      return this.keyBuffer.array();
    }

    @Override
    public int getRowOffset() {
      return getRowPosition();
    }

    @Override
    public short getRowLength() {
      return this.rowLength;
    }

    @Override
    public byte[] getFamilyArray() {
      return this.keyBuffer.array();
    }

    @Override
    public int getFamilyOffset() {
      return getFamilyPosition();
    }

    @Override
    public byte getFamilyLength() {
      return this.familyLength;
    }

    @Override
    public byte[] getQualifierArray() {
      return this.keyBuffer.array();
    }

    @Override
    public int getQualifierOffset() {
      return getQualifierPosition();
    }

    @Override
    public int getQualifierLength() {
      return this.qualifierLength;
    }

    @Override
    public long getTimestamp() {
      return this.timestamp;
    }

    @Override
    public byte getTypeByte() {
      return this.typeByte;
    }

    @Override
    public long getSequenceId() {
      return this.seqId;
    }

    @Override
    public byte[] getValueArray() {
      return CellUtil.cloneValue(this);
    }

    @Override
    public int getValueOffset() {
      return 0;
    }

    @Override
    public int getValueLength() {
      return this.valueLength;
    }

    @Override
    public byte[] getTagsArray() {
      return CellUtil.cloneTags(this);
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public int getTagsLength() {
      return this.tagsLength;
    }

    @Override
    public ByteBuffer getRowByteBuffer() {
      return this.keyBuffer;
    }

    @Override
    public int getRowPosition() {
      return Bytes.SIZEOF_SHORT;
    }

    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return this.keyBuffer;
    }

    @Override
    public int getFamilyPosition() {
      return this.familyOffset;
    }

    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return this.keyBuffer;
    }

    @Override
    public int getQualifierPosition() {
      return this.qualifierOffset;
    }

    @Override
    public ByteBuffer getValueByteBuffer() {
      return this.valueBuffer;
    }

    @Override
    public int getValuePosition() {
      return this.valueOffset;
    }

    @Override
    public ByteBuffer getTagsByteBuffer() {
      return this.tagsBuffer;
    }

    @Override
    public int getTagsPosition() {
      return this.tagsOffset;
    }

    @Override
    public long heapSize() {
      return FIXED_OVERHEAD + rowLength + familyLength + qualifierLength + valueLength + tagsLength;
    }

    @Override
    public void setSequenceId(long seqId) {
      this.seqId = seqId;
    }

    @Override
    public int write(OutputStream out) throws IOException {
      return write(out, true);
    }

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
      int lenToWrite = KeyValueUtil.length(rowLength, familyLength, qualifierLength, valueLength,
          tagsLength, withTags);
      ByteBufferUtils.putInt(out, lenToWrite);
      ByteBufferUtils.putInt(out, keyBuffer.capacity());
      ByteBufferUtils.putInt(out, valueLength);
      // Write key
      out.write(keyBuffer.array());
      // Write value
      ByteBufferUtils.copyBufferToStream(out, this.valueBuffer, this.valueOffset, this.valueLength);
      if (withTags) {
        // 2 bytes tags length followed by tags bytes
        // tags length is serialized with 2 bytes only(short way) even if the type is int.
        // As this is non -ve numbers, we save the sign bit. See HBASE-11437
        out.write((byte) (0xff & (this.tagsLength >> 8)));
        out.write((byte) (0xff & this.tagsLength));
        ByteBufferUtils.copyBufferToStream(out, this.tagsBuffer, this.tagsOffset, this.tagsLength);
      }
      return lenToWrite + Bytes.SIZEOF_INT;
    }
  }

  protected abstract static class
      BufferedEncodedSeeker<STATE extends SeekerState>
      implements EncodedSeeker {
    protected HFileBlockDecodingContext decodingCtx;
    protected final CellComparator comparator;
    protected ByteBuff currentBuffer;
    protected TagCompressionContext tagCompressionContext = null;
    protected  KeyValue.KeyOnlyKeyValue keyOnlyKV = new KeyValue.KeyOnlyKeyValue();
    // A temp pair object which will be reused by ByteBuff#asSubByteBuffer calls. This avoids too
    // many object creations.
    protected final ObjectIntPair<ByteBuffer> tmpPair = new ObjectIntPair<ByteBuffer>();
    protected STATE current, previous;

    public BufferedEncodedSeeker(CellComparator comparator,
        HFileBlockDecodingContext decodingCtx) {
      this.comparator = comparator;
      this.decodingCtx = decodingCtx;
      if (decodingCtx.getHFileContext().isCompressTags()) {
        try {
          tagCompressionContext = new TagCompressionContext(LRUDictionary.class, Byte.MAX_VALUE);
        } catch (Exception e) {
          throw new RuntimeException("Failed to initialize TagCompressionContext", e);
        }
      }
      current = createSeekerState(); // always valid
      previous = createSeekerState(); // may not be valid
    }

    protected boolean includesMvcc() {
      return this.decodingCtx.getHFileContext().isIncludesMvcc();
    }

    protected boolean includesTags() {
      return this.decodingCtx.getHFileContext().isIncludesTags();
    }

    @Override
    public int compareKey(CellComparator comparator, Cell key) {
      keyOnlyKV.setKey(current.keyBuffer, 0, current.keyLength);
      return comparator.compareKeyIgnoresMvcc(key, keyOnlyKV);
    }

    @Override
    public void setCurrentBuffer(ByteBuff buffer) {
      if (this.tagCompressionContext != null) {
        this.tagCompressionContext.clear();
      }
      currentBuffer = buffer;
      current.currentBuffer = currentBuffer;
      if(tagCompressionContext != null) {
        current.tagCompressionContext = tagCompressionContext;
      }
      decodeFirst();
      current.setKey(current.keyBuffer, current.memstoreTS);
      previous.invalidate();
    }

    @Override
    public Cell getKey() {
      byte[] key = new byte[current.keyLength];
      System.arraycopy(current.keyBuffer, 0, key, 0, current.keyLength);
      return new KeyValue.KeyOnlyKeyValue(key);
    }

    @Override
    public ByteBuffer getValueShallowCopy() {
      currentBuffer.asSubByteBuffer(current.valueOffset, current.valueLength, tmpPair);
      ByteBuffer dup = tmpPair.getFirst().duplicate();
      dup.position(tmpPair.getSecond());
      dup.limit(tmpPair.getSecond() + current.valueLength);
      return dup.slice();
    }

    @Override
    public Cell getCell() {
      return current.toCell();
    }

    @Override
    public void rewind() {
      currentBuffer.rewind();
      if (tagCompressionContext != null) {
        tagCompressionContext.clear();
      }
      decodeFirst();
      current.setKey(current.keyBuffer, current.memstoreTS);
      previous.invalidate();
    }

    @Override
    public boolean next() {
      if (!currentBuffer.hasRemaining()) {
        return false;
      }
      decodeNext();
      current.setKey(current.keyBuffer, current.memstoreTS);
      previous.invalidate();
      return true;
    }

    protected void decodeTags() {
      current.tagsLength = ByteBuff.readCompressedInt(currentBuffer);
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
          currentBuffer.skip(current.tagsCompressedLength);
          current.uncompressTags = true;// Reset this.
        }
        current.tagsOffset = -1;
      } else {
        // When tag compress is not used, let us not do copying of tags bytes into tagsBuffer.
        // Just mark the tags Offset so as to create the KV buffer later in getKeyValueBuffer()
        current.tagsOffset = currentBuffer.position();
        currentBuffer.skip(current.tagsLength);
      }
    }

    @Override
    public int seekToKeyInBlock(Cell seekCell, boolean seekBefore) {
      int rowCommonPrefix = 0;
      int familyCommonPrefix = 0;
      int qualCommonPrefix = 0;
      previous.invalidate();
      do {
        int comp;
        keyOnlyKV.setKey(current.keyBuffer, 0, current.keyLength);
        if (current.lastCommonPrefix != 0) {
          // The KV format has row key length also in the byte array. The
          // common prefix
          // includes it. So we need to subtract to find out the common prefix
          // in the
          // row part alone
          rowCommonPrefix = Math.min(rowCommonPrefix, current.lastCommonPrefix - 2);
        }
        if (current.lastCommonPrefix <= 2) {
          rowCommonPrefix = 0;
        }
        rowCommonPrefix += findCommonPrefixInRowPart(seekCell, keyOnlyKV, rowCommonPrefix);
        comp = compareCommonRowPrefix(seekCell, keyOnlyKV, rowCommonPrefix);
        if (comp == 0) {
          comp = compareTypeBytes(seekCell, keyOnlyKV);
          if (comp == 0) {
            // Subtract the fixed row key length and the family key fixed length
            familyCommonPrefix = Math.max(
                0,
                Math.min(familyCommonPrefix,
                    current.lastCommonPrefix - (3 + keyOnlyKV.getRowLength())));
            familyCommonPrefix += findCommonPrefixInFamilyPart(seekCell, keyOnlyKV,
                familyCommonPrefix);
            comp = compareCommonFamilyPrefix(seekCell, keyOnlyKV, familyCommonPrefix);
            if (comp == 0) {
              // subtract the rowkey fixed length and the family key fixed
              // length
              qualCommonPrefix = Math.max(
                  0,
                  Math.min(
                      qualCommonPrefix,
                      current.lastCommonPrefix
                          - (3 + keyOnlyKV.getRowLength() + keyOnlyKV.getFamilyLength())));
              qualCommonPrefix += findCommonPrefixInQualifierPart(seekCell, keyOnlyKV,
                  qualCommonPrefix);
              comp = compareCommonQualifierPrefix(seekCell, keyOnlyKV, qualCommonPrefix);
              if (comp == 0) {
                comp = CellComparator.compareTimestamps(seekCell, keyOnlyKV);
                if (comp == 0) {
                  // Compare types. Let the delete types sort ahead of puts;
                  // i.e. types
                  // of higher numbers sort before those of lesser numbers.
                  // Maximum
                  // (255)
                  // appears ahead of everything, and minimum (0) appears
                  // after
                  // everything.
                  comp = (0xff & keyOnlyKV.getTypeByte()) - (0xff & seekCell.getTypeByte());
                }
              }
            }
          }
        }
        if (comp == 0) { // exact match
          if (seekBefore) {
            if (!previous.isValid()) {
              // The caller (seekBefore) has to ensure that we are not at the
              // first key in the block.
              throw new IllegalStateException("Cannot seekBefore if "
                  + "positioned at the first key in the block: key="
                  + Bytes.toStringBinary(seekCell.getRowArray()));
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
          current.setKey(current.keyBuffer, current.memstoreTS);
        } else {
          break;
        }
      } while (true);

      // we hit the end of the block, not an exact match
      return 1;
    }

    private int compareTypeBytes(Cell key, Cell right) {
      if (key.getFamilyLength() + key.getQualifierLength() == 0
          && key.getTypeByte() == Type.Minimum.getCode()) {
        // left is "bigger", i.e. it appears later in the sorted order
        return 1;
      }
      if (right.getFamilyLength() + right.getQualifierLength() == 0
          && right.getTypeByte() == Type.Minimum.getCode()) {
        return -1;
      }
      return 0;
    }

    private static int findCommonPrefixInRowPart(Cell left, Cell right, int rowCommonPrefix) {
      return Bytes.findCommonPrefix(left.getRowArray(), right.getRowArray(), left.getRowLength()
          - rowCommonPrefix, right.getRowLength() - rowCommonPrefix, left.getRowOffset()
          + rowCommonPrefix, right.getRowOffset() + rowCommonPrefix);
    }

    private static int findCommonPrefixInFamilyPart(Cell left, Cell right, int familyCommonPrefix) {
      return Bytes
          .findCommonPrefix(left.getFamilyArray(), right.getFamilyArray(), left.getFamilyLength()
              - familyCommonPrefix, right.getFamilyLength() - familyCommonPrefix,
              left.getFamilyOffset() + familyCommonPrefix, right.getFamilyOffset()
                  + familyCommonPrefix);
    }

    private static int findCommonPrefixInQualifierPart(Cell left, Cell right,
        int qualifierCommonPrefix) {
      return Bytes.findCommonPrefix(left.getQualifierArray(), right.getQualifierArray(),
          left.getQualifierLength() - qualifierCommonPrefix, right.getQualifierLength()
              - qualifierCommonPrefix, left.getQualifierOffset() + qualifierCommonPrefix,
          right.getQualifierOffset() + qualifierCommonPrefix);
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
      // The current key has to be reset with the previous Cell
      current.setKey(current.keyBuffer, current.memstoreTS);
      previous.invalidate();
    }

    @SuppressWarnings("unchecked")
    protected STATE createSeekerState() {
      // This will fail for non-default seeker state if the subclass does not
      // override this method.
      return (STATE) new SeekerState(this.tmpPair, this.includesTags());
    }

    abstract protected void decodeFirst();
    abstract protected void decodeNext();
  }

  /**
   * @param cell
   * @param out
   * @param encodingCtx
   * @return unencoded size added
   * @throws IOException
   */
  protected final int afterEncodingKeyValue(Cell cell, DataOutputStream out,
      HFileBlockDefaultEncodingContext encodingCtx) throws IOException {
    int size = 0;
    if (encodingCtx.getHFileContext().isIncludesTags()) {
      int tagsLength = cell.getTagsLength();
      ByteBufferUtils.putCompressedInt(out, tagsLength);
      // There are some tags to be written
      if (tagsLength > 0) {
        TagCompressionContext tagCompressionContext = encodingCtx.getTagCompressionContext();
        // When tag compression is enabled, tagCompressionContext will have a not null value. Write
        // the tags using Dictionary compression in such a case
        if (tagCompressionContext != null) {
          // Not passing tagsLength considering that parsing of the tagsLength is not costly
          CellUtil.compressTags(out, cell, tagCompressionContext);
        } else {
          CellUtil.writeTags(out, cell, tagsLength);
        }
      }
      size += tagsLength + KeyValue.TAGS_LENGTH_SIZE;
    }
    if (encodingCtx.getHFileContext().isIncludesMvcc()) {
      // Copy memstore timestamp from the byte buffer to the output stream.
      long memstoreTS = cell.getSequenceId();
      WritableUtils.writeVLong(out, memstoreTS);
      // TODO use a writeVLong which returns the #bytes written so that 2 time parsing can be
      // avoided.
      size += WritableUtils.getVIntSize(memstoreTS);
    }
    return size;
  }

  protected final void afterDecodingKeyValue(DataInputStream source,
      ByteBuffer dest, HFileBlockDefaultDecodingContext decodingCtx) throws IOException {
    if (decodingCtx.getHFileContext().isIncludesTags()) {
      int tagsLength = ByteBufferUtils.readCompressedInt(source);
      // Put as unsigned short
      dest.put((byte) ((tagsLength >> 8) & 0xff));
      dest.put((byte) (tagsLength & 0xff));
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

  protected abstract ByteBuffer internalDecodeKeyValues(DataInputStream source,
      int allocateHeaderLength, int skipLastBytes, HFileBlockDefaultDecodingContext decodingCtx)
      throws IOException;

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

  @Override
  public void startBlockEncoding(HFileBlockEncodingContext blkEncodingCtx, DataOutputStream out)
      throws IOException {
    if (blkEncodingCtx.getClass() != HFileBlockDefaultEncodingContext.class) {
      throw new IOException (this.getClass().getName() + " only accepts "
          + HFileBlockDefaultEncodingContext.class.getName() + " as the " +
          "encoding context.");
    }

    HFileBlockDefaultEncodingContext encodingCtx =
        (HFileBlockDefaultEncodingContext) blkEncodingCtx;
    encodingCtx.prepareEncoding(out);
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
    StreamUtils.writeInt(out, 0); // DUMMY length. This will be updated in endBlockEncoding()
    blkEncodingCtx.setEncodingState(new BufferedDataBlockEncodingState());
  }

  private static class BufferedDataBlockEncodingState extends EncodingState {
    int unencodedDataSizeWritten = 0;
  }

  @Override
  public int encode(Cell cell, HFileBlockEncodingContext encodingCtx, DataOutputStream out)
      throws IOException {
    BufferedDataBlockEncodingState state = (BufferedDataBlockEncodingState) encodingCtx
        .getEncodingState();
    int encodedKvSize = internalEncode(cell, (HFileBlockDefaultEncodingContext) encodingCtx, out);
    state.unencodedDataSizeWritten += encodedKvSize;
    return encodedKvSize;
  }

  public abstract int internalEncode(Cell cell, HFileBlockDefaultEncodingContext encodingCtx,
      DataOutputStream out) throws IOException;

  @Override
  public void endBlockEncoding(HFileBlockEncodingContext encodingCtx, DataOutputStream out,
      byte[] uncompressedBytesWithHeader) throws IOException {
    BufferedDataBlockEncodingState state = (BufferedDataBlockEncodingState) encodingCtx
        .getEncodingState();
    // Write the unencodedDataSizeWritten (with header size)
    Bytes.putInt(uncompressedBytesWithHeader,
      HConstants.HFILEBLOCK_HEADER_SIZE + DataBlockEncoding.ID_SIZE, state.unencodedDataSizeWritten
        );
    if (encodingCtx.getDataBlockEncoding() != DataBlockEncoding.NONE) {
      encodingCtx.postEncoding(BlockType.ENCODED_DATA);
    } else {
      encodingCtx.postEncoding(BlockType.DATA);
    }
  }

  protected Cell createFirstKeyCell(ByteBuffer key, int keyLength) {
    if (key.hasArray()) {
      return new KeyValue.KeyOnlyKeyValue(key.array(), key.arrayOffset() + key.position(),
          keyLength);
    } else {
      return new ByteBufferedKeyOnlyKeyValue(key, key.position(), keyLength);
    }
  }
}
