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
package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.regionserver.BloomType.ROWPREFIX_FIXED_LENGTH;

import java.text.NumberFormat;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods related to BloomFilters
 */
@InterfaceAudience.Private
public final class BloomFilterUtil {

  /** Record separator for the Bloom filter statistics human-readable string */
  public static final String STATS_RECORD_SEP = "; ";
  /**
   * Used in computing the optimal Bloom filter size. This approximately equals 0.480453.
   */
  public static final double LOG2_SQUARED = Math.log(2) * Math.log(2);

  /**
   * A random number generator to use for "fake lookups" when testing to estimate the ideal false
   * positive rate.
   */
  private static Random randomGeneratorForTest;

  public static final String PREFIX_LENGTH_KEY = "RowPrefixBloomFilter.prefix_length";

  /** Bit-value lookup array to prevent doing the same work over and over */
  public static final byte[] bitvals = { (byte) 0x01, (byte) 0x02, (byte) 0x04, (byte) 0x08,
    (byte) 0x10, (byte) 0x20, (byte) 0x40, (byte) 0x80 };

  /**
   * Private constructor to keep this class from being instantiated.
   */
  private BloomFilterUtil() {
  }

  /**
   * nn * @return the number of bits for a Bloom filter than can hold the given number of keys and
   * provide the given error rate, assuming that the optimal number of hash functions is used and it
   * does not have to be an integer.
   */
  public static long computeBitSize(long maxKeys, double errorRate) {
    return (long) Math.ceil(maxKeys * (-Math.log(errorRate) / LOG2_SQUARED));
  }

  /**
   * Sets a random generator to be used for look-ups instead of computing hashes. Can be used to
   * simulate uniformity of accesses better in a test environment. Should not be set in a real
   * environment where correctness matters!
   * <p>
   * This gets used in {@link #contains(ByteBuff, int, int, Hash, int, HashKey)}
   * @param random The random number source to use, or null to compute actual hashes
   */
  public static void setRandomGeneratorForTest(Random random) {
    randomGeneratorForTest = random;
  }

  /**
   * The maximum number of keys we can put into a Bloom filter of a certain size to maintain the
   * given error rate, assuming the number of hash functions is chosen optimally and does not even
   * have to be an integer (hence the "ideal" in the function name). nn * @return maximum number of
   * keys that can be inserted into the Bloom filter
   * @see #computeMaxKeys(long, double, int) for a more precise estimate
   */
  public static long idealMaxKeys(long bitSize, double errorRate) {
    // The reason we need to use floor here is that otherwise we might put
    // more keys in a Bloom filter than is allowed by the target error rate.
    return (long) (bitSize * (LOG2_SQUARED / -Math.log(errorRate)));
  }

  /**
   * The maximum number of keys we can put into a Bloom filter of a certain size to get the given
   * error rate, with the given number of hash functions. nnn * @return the maximum number of keys
   * that can be inserted in a Bloom filter to maintain the target error rate, if the number of hash
   * functions is provided.
   */
  public static long computeMaxKeys(long bitSize, double errorRate, int hashCount) {
    return (long) (-bitSize * 1.0 / hashCount
      * Math.log(1 - Math.exp(Math.log(errorRate) / hashCount)));
  }

  /**
   * Computes the actual error rate for the given number of elements, number of bits, and number of
   * hash functions. Taken directly from the
   * <a href= "http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives" > Wikipedia
   * Bloom filter article</a>. nnn * @return the actual error rate
   */
  public static double actualErrorRate(long maxKeys, long bitSize, int functionCount) {
    return Math
      .exp(Math.log(1 - Math.exp(-functionCount * maxKeys * 1.0 / bitSize)) * functionCount);
  }

  /**
   * Increases the given byte size of a Bloom filter until it can be folded by the given factor. nn
   * * @return Foldable byte size
   */
  public static int computeFoldableByteSize(long bitSize, int foldFactor) {
    long byteSizeLong = (bitSize + 7) / 8;
    int mask = (1 << foldFactor) - 1;
    if ((mask & byteSizeLong) != 0) {
      byteSizeLong >>= foldFactor;
      ++byteSizeLong;
      byteSizeLong <<= foldFactor;
    }
    if (byteSizeLong > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("byteSize=" + byteSizeLong + " too " + "large for bitSize="
        + bitSize + ", foldFactor=" + foldFactor);
    }
    return (int) byteSizeLong;
  }

  public static int optimalFunctionCount(int maxKeys, long bitSize) {
    long i = bitSize / maxKeys;
    double result = Math.ceil(Math.log(2) * i);
    if (result > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("result too large for integer value.");
    }
    return (int) result;
  }

  /**
   * Creates a Bloom filter chunk of the given size.
   * @param byteSizeHint the desired number of bytes for the Bloom filter bit array. Will be
   *                     increased so that folding is possible.
   * @param errorRate    target false positive rate of the Bloom filter
   * @param hashType     Bloom filter hash function type nn * @return the new Bloom filter of the
   *                     desired size
   */
  public static BloomFilterChunk createBySize(int byteSizeHint, double errorRate, int hashType,
    int foldFactor, BloomType bloomType) {
    BloomFilterChunk bbf = new BloomFilterChunk(hashType, bloomType);

    bbf.byteSize = computeFoldableByteSize(byteSizeHint * 8L, foldFactor);
    long bitSize = bbf.byteSize * 8;
    bbf.maxKeys = (int) idealMaxKeys(bitSize, errorRate);
    bbf.hashCount = optimalFunctionCount(bbf.maxKeys, bitSize);

    // Adjust max keys to bring error rate closer to what was requested,
    // because byteSize was adjusted to allow for folding, and hashCount was
    // rounded.
    bbf.maxKeys = (int) computeMaxKeys(bitSize, errorRate, bbf.hashCount);

    return bbf;
  }

  public static boolean contains(byte[] buf, int offset, int length, ByteBuff bloomBuf,
    int bloomOffset, int bloomSize, Hash hash, int hashCount) {
    HashKey<byte[]> hashKey = new ByteArrayHashKey(buf, offset, length);
    return contains(bloomBuf, bloomOffset, bloomSize, hash, hashCount, hashKey);
  }

  private static <T> boolean contains(ByteBuff bloomBuf, int bloomOffset, int bloomSize, Hash hash,
    int hashCount, HashKey<T> hashKey) {
    int hash1 = hash.hash(hashKey, 0);
    int bloomBitSize = bloomSize << 3;

    int hash2 = 0;
    int compositeHash = 0;

    if (randomGeneratorForTest == null) {
      // Production mode
      compositeHash = hash1;
      hash2 = hash.hash(hashKey, hash1);
    }

    for (int i = 0; i < hashCount; i++) {
      int hashLoc = (randomGeneratorForTest == null
        // Production mode
        ? Math.abs(compositeHash % bloomBitSize)
        // Test mode with "fake look-ups" to estimate "ideal false positive rate"
        : randomGeneratorForTest.nextInt(bloomBitSize));
      compositeHash += hash2;
      if (!checkBit(hashLoc, bloomBuf, bloomOffset)) {
        return false;
      }
    }
    return true;
  }

  public static boolean contains(Cell cell, ByteBuff bloomBuf, int bloomOffset, int bloomSize,
    Hash hash, int hashCount, BloomType type) {
    HashKey<Cell> hashKey =
      type == BloomType.ROWCOL ? new RowColBloomHashKey(cell) : new RowBloomHashKey(cell);
    return contains(bloomBuf, bloomOffset, bloomSize, hash, hashCount, hashKey);
  }

  /**
   * Check if bit at specified index is 1.
   * @param pos index of bit
   * @return true if bit at specified index is 1, false if 0.
   */
  static boolean checkBit(int pos, ByteBuff bloomBuf, int bloomOffset) {
    int bytePos = pos >> 3; // pos / 8
    int bitPos = pos & 0x7; // pos % 8
    byte curByte = bloomBuf.get(bloomOffset + bytePos);
    curByte &= bitvals[bitPos];
    return (curByte != 0);
  }

  /**
   * A human-readable string with statistics for the given Bloom filter.
   * @param bloomFilter the Bloom filter to output statistics for;
   * @return a string consisting of "&lt;key&gt;: &lt;value&gt;" parts separated by
   *         {@link #STATS_RECORD_SEP}.
   */
  public static String formatStats(BloomFilterBase bloomFilter) {
    StringBuilder sb = new StringBuilder();
    long k = bloomFilter.getKeyCount();
    long m = bloomFilter.getMaxKeys();

    sb.append("BloomSize: " + bloomFilter.getByteSize() + STATS_RECORD_SEP);
    sb.append("No of Keys in bloom: " + k + STATS_RECORD_SEP);
    sb.append("Max Keys for bloom: " + m);
    if (m > 0) {
      sb.append(STATS_RECORD_SEP + "Percentage filled: "
        + NumberFormat.getPercentInstance().format(k * 1.0 / m));
    }
    return sb.toString();
  }

  public static String toString(BloomFilterChunk bloomFilter) {
    return formatStats(bloomFilter) + STATS_RECORD_SEP + "Actual error rate: "
      + String.format("%.8f", bloomFilter.actualErrorRate());
  }

  public static byte[] getBloomFilterParam(BloomType bloomFilterType, Configuration conf)
    throws IllegalArgumentException {
    byte[] bloomParam = null;
    String message = "Bloom filter type is " + bloomFilterType + ", ";
    if (bloomFilterType.equals(ROWPREFIX_FIXED_LENGTH)) {
      String prefixLengthString = conf.get(PREFIX_LENGTH_KEY);
      if (prefixLengthString == null) {
        message += PREFIX_LENGTH_KEY + " not specified.";
        throw new IllegalArgumentException(message);
      }
      int prefixLength;
      try {
        prefixLength = Integer.parseInt(prefixLengthString);
        if (prefixLength <= 0 || prefixLength > HConstants.MAX_ROW_LENGTH) {
          message +=
            "the value of " + PREFIX_LENGTH_KEY + " must >=0 and < " + HConstants.MAX_ROW_LENGTH;
          throw new IllegalArgumentException(message);
        }
      } catch (NumberFormatException nfe) {
        message = "Number format exception when parsing " + PREFIX_LENGTH_KEY + " for BloomType "
          + bloomFilterType.toString() + ":" + prefixLengthString;
        throw new IllegalArgumentException(message, nfe);
      }
      bloomParam = Bytes.toBytes(prefixLength);
    }
    return bloomParam;
  }
}
