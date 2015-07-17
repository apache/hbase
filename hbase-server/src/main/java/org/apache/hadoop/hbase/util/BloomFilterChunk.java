/*
 *
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

package org.apache.hadoop.hbase.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * The basic building block for the {@link org.apache.hadoop.hbase.io.hfile.CompoundBloomFilter}
 */
@InterfaceAudience.Private
public class BloomFilterChunk implements BloomFilterBase {

  /** Bytes (B) in the array. This actually has to fit into an int. */
  protected long byteSize;
  /** Number of hash functions */
  protected int hashCount;
  /** Hash type */
  protected final int hashType;
  /** Hash Function */
  protected final Hash hash;
  /** Keys currently in the bloom */
  protected int keyCount;
  /** Max Keys expected for the bloom */
  protected int maxKeys;
  /** Bloom bits */
  protected ByteBuffer bloom;

  /**
   * Loads bloom filter meta data from file input.
   * @param meta stored bloom meta data
   * @throws IllegalArgumentException meta data is invalid
   */
  public BloomFilterChunk(DataInput meta)
      throws IOException, IllegalArgumentException {
    this.byteSize = meta.readInt();
    this.hashCount = meta.readInt();
    this.hashType = meta.readInt();
    this.keyCount = meta.readInt();
    this.maxKeys = this.keyCount;

    this.hash = Hash.getInstance(this.hashType);
    if (hash == null) {
      throw new IllegalArgumentException("Invalid hash type: " + hashType);
    }
    sanityCheck();
  }

  /**
   * Computes the error rate for this Bloom filter, taking into account the
   * actual number of hash functions and keys inserted. The return value of
   * this function changes as a Bloom filter is being populated. Used for
   * reporting the actual error rate of compound Bloom filters when writing
   * them out.
   *
   * @return error rate for this particular Bloom filter
   */
  public double actualErrorRate() {
    return BloomFilterUtil.actualErrorRate(keyCount, byteSize * 8, hashCount);
  }

  public BloomFilterChunk(int hashType) {
    this.hashType = hashType;
    this.hash = Hash.getInstance(hashType);
  }

  /**
   * Determines &amp; initializes bloom filter meta data from user config. Call
   * {@link #allocBloom()} to allocate bloom filter data.
   *
   * @param maxKeys Maximum expected number of keys that will be stored in this
   *          bloom
   * @param errorRate Desired false positive error rate. Lower rate = more
   *          storage required
   * @param hashType Type of hash function to use
   * @param foldFactor When finished adding entries, you may be able to 'fold'
   *          this bloom to save space. Tradeoff potentially excess bytes in
   *          bloom for ability to fold if keyCount is exponentially greater
   *          than maxKeys.
   * @throws IllegalArgumentException
   */
  public BloomFilterChunk(int maxKeys, double errorRate, int hashType,
      int foldFactor) throws IllegalArgumentException {
    this(hashType);

    long bitSize = BloomFilterUtil.computeBitSize(maxKeys, errorRate);
    hashCount = BloomFilterUtil.optimalFunctionCount(maxKeys, bitSize);
    this.maxKeys = maxKeys;

    // increase byteSize so folding is possible
    byteSize = BloomFilterUtil.computeFoldableByteSize(bitSize, foldFactor);

    sanityCheck();
  }

  /**
   * Creates another similar Bloom filter. Does not copy the actual bits, and
   * sets the new filter's key count to zero.
   *
   * @return a Bloom filter with the same configuration as this
   */
  public BloomFilterChunk createAnother() {
    BloomFilterChunk bbf = new BloomFilterChunk(hashType);
    bbf.byteSize = byteSize;
    bbf.hashCount = hashCount;
    bbf.maxKeys = maxKeys;
    return bbf;
  }

  public void allocBloom() {
    if (this.bloom != null) {
      throw new IllegalArgumentException("can only create bloom once.");
    }
    this.bloom = ByteBuffer.allocate((int)this.byteSize);
    assert this.bloom.hasArray();
  }

  void sanityCheck() throws IllegalArgumentException {
    if(0 >= this.byteSize || this.byteSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Invalid byteSize: " + this.byteSize);
    }

    if(this.hashCount <= 0) {
      throw new IllegalArgumentException("Hash function count must be > 0");
    }

    if (this.hash == null) {
      throw new IllegalArgumentException("hashType must be known");
    }

    if (this.keyCount < 0) {
      throw new IllegalArgumentException("must have positive keyCount");
    }
  }

  void bloomCheck(ByteBuffer bloom)  throws IllegalArgumentException {
    if (this.byteSize != bloom.limit()) {
      throw new IllegalArgumentException(
          "Configured bloom length should match actual length");
    }
  }

  public void add(byte [] buf) {
    add(buf, 0, buf.length);
  }

  public void add(byte [] buf, int offset, int len) {
    /*
     * For faster hashing, use combinatorial generation
     * http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
     */
    int hash1 = this.hash.hash(buf, offset, len, 0);
    int hash2 = this.hash.hash(buf, offset, len, hash1);

    for (int i = 0; i < this.hashCount; i++) {
      long hashLoc = Math.abs((hash1 + i * hash2) % (this.byteSize * 8));
      set(hashLoc);
    }

    ++this.keyCount;
  }

  //---------------------------------------------------------------------------
  /** Private helpers */

  /**
   * Set the bit at the specified index to 1.
   *
   * @param pos index of bit
   */
  void set(long pos) {
    int bytePos = (int)(pos / 8);
    int bitPos = (int)(pos % 8);
    byte curByte = bloom.get(bytePos);
    curByte |= BloomFilterUtil.bitvals[bitPos];
    bloom.put(bytePos, curByte);
  }

  /**
   * Check if bit at specified index is 1.
   *
   * @param pos index of bit
   * @return true if bit at specified index is 1, false if 0.
   */
  static boolean get(int pos, ByteBuffer bloomBuf, int bloomOffset) {
    int bytePos = pos >> 3; //pos / 8
    int bitPos = pos & 0x7; //pos % 8
    // TODO access this via Util API which can do Unsafe access if possible(?)
    byte curByte = bloomBuf.get(bloomOffset + bytePos);
    curByte &= BloomFilterUtil.bitvals[bitPos];
    return (curByte != 0);
  }

  @Override
  public long getKeyCount() {
    return keyCount;
  }

  @Override
  public long getMaxKeys() {
    return maxKeys;
  }

  @Override
  public long getByteSize() {
    return byteSize;
  }

  public int getHashType() {
    return hashType;
  }

  public void compactBloom() {
    // see if the actual size is exponentially smaller than expected.
    if (this.keyCount > 0 && this.bloom.hasArray()) {
      int pieces = 1;
      int newByteSize = (int)this.byteSize;
      int newMaxKeys = this.maxKeys;

      // while exponentially smaller & folding is lossless
      while ((newByteSize & 1) == 0 && newMaxKeys > (this.keyCount<<1)) {
        pieces <<= 1;
        newByteSize >>= 1;
        newMaxKeys >>= 1;
      }

      // if we should fold these into pieces
      if (pieces > 1) {
        byte[] array = this.bloom.array();
        int start = this.bloom.arrayOffset();
        int end = start + newByteSize;
        int off = end;
        for(int p = 1; p < pieces; ++p) {
          for(int pos = start; pos < end; ++pos) {
            array[pos] |= array[off++];
          }
        }
        // folding done, only use a subset of this array
        this.bloom.rewind();
        this.bloom.limit(newByteSize);
        this.bloom = this.bloom.slice();
        this.byteSize = newByteSize;
        this.maxKeys = newMaxKeys;
      }
    }
  }

  /**
   * Writes just the bloom filter to the output array
   * @param out OutputStream to place bloom
   * @throws IOException Error writing bloom array
   */
  public void writeBloom(final DataOutput out)
      throws IOException {
    if (!this.bloom.hasArray()) {
      throw new IOException("Only writes ByteBuffer with underlying array.");
    }
    out.write(this.bloom.array(), this.bloom.arrayOffset(), this.bloom.limit());
  }

  public int getHashCount() {
    return hashCount;
  }

  @Override
  public String toString() {
    return BloomFilterUtil.toString(this);
  }

}
