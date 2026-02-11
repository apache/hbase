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

import static java.lang.Integer.toUnsignedLong;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * XXH3 64-bit hash implementation.
 * <p>
 * Optimized for modern CPU architectures, providing high throughput, strong dispersion, and minimal
 * memory usage. Designed for low-latency, zero-allocation hashing in most cases.
 * <p>
 * Note, however, that when the input exceeds 240 bytes and a non-default seed (anything other than
 * {@code 0}) is used, a derived secret must be generated internally, which results in a temporary
 * byte-array allocation.
 * @see <a href=
 *      "https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md#xxh3-algorithm-overview">XXH3
 *      Algorithm</a>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class XXH3 extends Hash implements Hash64 {
  private static final XXH3 _instance = new XXH3();
  private static final long MASK32 = 0xFFFFFFFFL;
  private static final long PRIME32_1 = 0x9E3779B1L;
  private static final long PRIME32_2 = 0x85EBCA77L;
  private static final long PRIME32_3 = 0xC2B2AE3DL;
  private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
  private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
  private static final long PRIME64_3 = 0x165667B19E3779F9L;
  private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
  private static final long PRIME64_5 = 0x27D4EB2F165667C5L;
  private static final long PRIME_MX1 = 0x165667919E3779F9L;
  private static final long PRIME_MX2 = 0x9FB21C651E98DF25L;

  private static final byte[] DEFAULT_SECRET = { (byte) 0xb8, (byte) 0xfe, (byte) 0x6c, (byte) 0x39,
    (byte) 0x23, (byte) 0xa4, (byte) 0x4b, (byte) 0xbe, (byte) 0x7c, (byte) 0x01, (byte) 0x81,
    (byte) 0x2c, (byte) 0xf7, (byte) 0x21, (byte) 0xad, (byte) 0x1c, (byte) 0xde, (byte) 0xd4,
    (byte) 0x6d, (byte) 0xe9, (byte) 0x83, (byte) 0x90, (byte) 0x97, (byte) 0xdb, (byte) 0x72,
    (byte) 0x40, (byte) 0xa4, (byte) 0xa4, (byte) 0xb7, (byte) 0xb3, (byte) 0x67, (byte) 0x1f,
    (byte) 0xcb, (byte) 0x79, (byte) 0xe6, (byte) 0x4e, (byte) 0xcc, (byte) 0xc0, (byte) 0xe5,
    (byte) 0x78, (byte) 0x82, (byte) 0x5a, (byte) 0xd0, (byte) 0x7d, (byte) 0xcc, (byte) 0xff,
    (byte) 0x72, (byte) 0x21, (byte) 0xb8, (byte) 0x08, (byte) 0x46, (byte) 0x74, (byte) 0xf7,
    (byte) 0x43, (byte) 0x24, (byte) 0x8e, (byte) 0xe0, (byte) 0x35, (byte) 0x90, (byte) 0xe6,
    (byte) 0x81, (byte) 0x3a, (byte) 0x26, (byte) 0x4c, (byte) 0x3c, (byte) 0x28, (byte) 0x52,
    (byte) 0xbb, (byte) 0x91, (byte) 0xc3, (byte) 0x00, (byte) 0xcb, (byte) 0x88, (byte) 0xd0,
    (byte) 0x65, (byte) 0x8b, (byte) 0x1b, (byte) 0x53, (byte) 0x2e, (byte) 0xa3, (byte) 0x71,
    (byte) 0x64, (byte) 0x48, (byte) 0x97, (byte) 0xa2, (byte) 0x0d, (byte) 0xf9, (byte) 0x4e,
    (byte) 0x38, (byte) 0x19, (byte) 0xef, (byte) 0x46, (byte) 0xa9, (byte) 0xde, (byte) 0xac,
    (byte) 0xd8, (byte) 0xa8, (byte) 0xfa, (byte) 0x76, (byte) 0x3f, (byte) 0xe3, (byte) 0x9c,
    (byte) 0x34, (byte) 0x3f, (byte) 0xf9, (byte) 0xdc, (byte) 0xbb, (byte) 0xc7, (byte) 0xc7,
    (byte) 0x0b, (byte) 0x4f, (byte) 0x1d, (byte) 0x8a, (byte) 0x51, (byte) 0xe0, (byte) 0x4b,
    (byte) 0xcd, (byte) 0xb4, (byte) 0x59, (byte) 0x31, (byte) 0xc8, (byte) 0x9f, (byte) 0x7e,
    (byte) 0xc9, (byte) 0xd9, (byte) 0x78, (byte) 0x73, (byte) 0x64, (byte) 0xea, (byte) 0xc5,
    (byte) 0xac, (byte) 0x83, (byte) 0x34, (byte) 0xd3, (byte) 0xeb, (byte) 0xc3, (byte) 0xc5,
    (byte) 0x81, (byte) 0xa0, (byte) 0xff, (byte) 0xfa, (byte) 0x13, (byte) 0x63, (byte) 0xeb,
    (byte) 0x17, (byte) 0x0d, (byte) 0xdd, (byte) 0x51, (byte) 0xb7, (byte) 0xf0, (byte) 0xda,
    (byte) 0x49, (byte) 0xd3, (byte) 0x16, (byte) 0x55, (byte) 0x26, (byte) 0x29, (byte) 0xd4,
    (byte) 0x68, (byte) 0x9e, (byte) 0x2b, (byte) 0x16, (byte) 0xbe, (byte) 0x58, (byte) 0x7d,
    (byte) 0x47, (byte) 0xa1, (byte) 0xfc, (byte) 0x8f, (byte) 0xf8, (byte) 0xb8, (byte) 0xd1,
    (byte) 0x7a, (byte) 0xd0, (byte) 0x31, (byte) 0xce, (byte) 0x45, (byte) 0xcb, (byte) 0x3a,
    (byte) 0x8f, (byte) 0x95, (byte) 0x16, (byte) 0x04, (byte) 0x28, (byte) 0xaf, (byte) 0xd7,
    (byte) 0xfb, (byte) 0xca, (byte) 0xbb, (byte) 0x4b, (byte) 0x40, (byte) 0x7e, };

  // Pre-converted longs from DefaultSecret to avoid reconstruction at runtime
  private static final long DEFAULT_SECRET_LONG_0 = 0xBE4BA423396CFEB8L;
  private static final long DEFAULT_SECRET_LONG_1 = 0x1CAD21F72C81017CL;
  private static final long DEFAULT_SECRET_LONG_2 = 0xDB979083E96DD4DEL;
  private static final long DEFAULT_SECRET_LONG_3 = 0x1F67B3B7A4A44072L;
  private static final long DEFAULT_SECRET_LONG_4 = 0x78E5C0CC4EE679CBL;
  private static final long DEFAULT_SECRET_LONG_5 = 0x2172FFCC7DD05A82L;
  private static final long DEFAULT_SECRET_LONG_6 = 0x8E2443F7744608B8L;
  private static final long DEFAULT_SECRET_LONG_7 = 0x4C263A81E69035E0L;
  private static final long DEFAULT_SECRET_LONG_8 = 0xCB00C391BB52283CL;
  private static final long DEFAULT_SECRET_LONG_9 = 0xA32E531B8B65D088L;
  private static final long DEFAULT_SECRET_LONG_10 = 0x4EF90DA297486471L;
  private static final long DEFAULT_SECRET_LONG_11 = 0xD8ACDEA946EF1938L;
  private static final long DEFAULT_SECRET_LONG_12 = 0x3F349CE33F76FAA8L;
  private static final long DEFAULT_SECRET_LONG_13 = 0x1D4F0BC7C7BBDCF9L;
  private static final long DEFAULT_SECRET_LONG_14 = 0x3159B4CD4BE0518AL;
  private static final long DEFAULT_SECRET_LONG_15 = 0x647378D9C97E9FC8L;

  // Pre-converted longs from DefaultSecret (3-byte offset) to avoid reconstruction at runtime.
  private static final long DEFAULT_SECRET_3_LONG_0 = 0x81017CBE4BA42339L;
  private static final long DEFAULT_SECRET_3_LONG_1 = 0x6DD4DE1CAD21F72CL;
  private static final long DEFAULT_SECRET_3_LONG_2 = 0xA44072DB979083E9L;
  private static final long DEFAULT_SECRET_3_LONG_3 = 0xE679CB1F67B3B7A4L;
  private static final long DEFAULT_SECRET_3_LONG_4 = 0xD05A8278E5C0CC4EL;
  private static final long DEFAULT_SECRET_3_LONG_5 = 0x4608B82172FFCC7DL;
  private static final long DEFAULT_SECRET_3_LONG_6 = 0x9035E08E2443F774L;
  private static final long DEFAULT_SECRET_3_LONG_7 = 0x52283C4C263A81E6L;
  private static final long DEFAULT_SECRET_3_LONG_8 = 0x65D088CB00C391BBL;
  private static final long DEFAULT_SECRET_3_LONG_9 = 0x486471A32E531B8BL;
  private static final long DEFAULT_SECRET_3_LONG_10 = 0xEF19384EF90DA297L;
  private static final long DEFAULT_SECRET_3_LONG_11 = 0x76FAA8D8ACDEA946L;
  private static final long DEFAULT_SECRET_3_LONG_12 = 0xBBDCF93F349CE33FL;
  private static final long DEFAULT_SECRET_3_LONG_13 = 0xE0518A1D4F0BC7C7L;

  private static final long DEFAULT_SECRET_LONG_119 = 0x7378D9C97E9FC831L;
  private static final long DEFAULT_SECRET_LONG_127 = 0xEBD33483ACC5EA64L;

  private static final long DEFAULT_SECRET_BITFLIP_0 =
    ((DEFAULT_SECRET_LONG_0 >>> 32) ^ (DEFAULT_SECRET_LONG_0 & MASK32));
  private static final long DEFAULT_SECRET_BITFLIP_1 = (DEFAULT_SECRET_LONG_1 ^ DEFAULT_SECRET_LONG_2);
  private static final long DEFAULT_SECRET_BITFLIP_2 = (DEFAULT_SECRET_LONG_3 ^ DEFAULT_SECRET_LONG_4);
  private static final long DEFAULT_SECRET_BITFLIP_3 = (DEFAULT_SECRET_LONG_5 ^ DEFAULT_SECRET_LONG_6);
  private static final long DEFAULT_SECRET_BITFLIP_4 = (DEFAULT_SECRET_LONG_7 ^ DEFAULT_SECRET_LONG_8);

  public static Hash getInstance() {
    return _instance;
  }

  @Override
  public <T> long hash64(HashKey<T> hashKey, long seed) {
    int length = hashKey.length();
    long result64;

    if (length <= 16) {
      result64 = hashSmall(hashKey, length, seed);
    } else if (length <= 240) {
      result64 = hashMedium(hashKey, length, seed);
    } else {
      result64 = hashLarge(hashKey, length, seed);
    }

    return result64;
  }

  @Override
  public <T> int hash(HashKey<T> hashKey, int seed) {
    long result64 = hash64(hashKey, (long) seed);
    return toLow32Int(result64);
  }

  private byte[] initSecret(long seed) {
    if (seed == 0L) {
      return DEFAULT_SECRET;
    } else {
      // For non-default seeds, derived secret generation requires a new byte array.
      byte[] derivedSecret = new byte[192];
      for (int i = 0; i < 12; i++) {
        putLong64LE(derivedSecret, i * 16, readLong64LE(DEFAULT_SECRET, i * 16) + seed);
        putLong64LE(derivedSecret, i * 16 + 8, readLong64LE(DEFAULT_SECRET, i * 16 + 8) - seed);
      }
      return derivedSecret;
    }
  }

  private static int toLow32Int(long value) {
    // Grab only the lower 32 bits.
    return (int) value;
  }

  private static long avalanche(long hash) {
    hash ^= hash >>> 37;
    hash *= PRIME_MX1;
    hash ^= hash >>> 32;
    return hash;
  }

  private static long avalancheXXH64(long hash) {
    hash ^= hash >>> 33;
    hash *= PRIME64_2;
    hash ^= hash >>> 29;
    hash *= PRIME64_3;
    hash ^= hash >>> 32;
    return hash;
  }

  private static long rrmxmx(long hash, int inputLength) {
    hash ^= Long.rotateLeft(hash, 49) ^ Long.rotateLeft(hash, 24);
    hash *= PRIME_MX2;
    hash ^= (hash >>> 35) + inputLength;
    hash *= PRIME_MX2;
    hash ^= hash >>> 28;
    return hash;
  }

  private static long mul128AndFold64(long x, long y) {
    long xLow = x & MASK32;
    long xHigh = x >>> 32;
    long yLow = y & MASK32;
    long yHigh = y >>> 32;

    long lowLow = xLow * yLow;
    long lowHigh = xLow * yHigh;
    long highLow = xHigh * yLow;
    long highHigh = xHigh * yHigh;

    long mid = lowHigh + (highLow & MASK32) + (lowLow >>> 32);
    long hi = highHigh + (highLow >>> 32) + (mid >>> 32);
    long lo = (mid << 32) | (lowLow & MASK32);

    return hi ^ lo;
  }

  private static <T> long mix16(HashKey<T> key, int keyOffset, long secretLow, long secretHigh,
    long seed) {
    long inputLow = readLong64LE(key, keyOffset);
    long inputHigh = readLong64LE(key, keyOffset + 8);
    return mul128AndFold64(inputLow ^ (secretLow + seed), inputHigh ^ (secretHigh - seed));
  }

  private static <T> int readInt32LE(HashKey<T> key, int offset) {
    return key.getIntLE(offset);
  }

  private static <T> long readLong64LE(HashKey<T> key, int offset) {
    return key.getLongLE(offset);
  }

  private static long readLong64LE(byte[] input, int offset) {
    return LittleEndianBytes.toLong(input, offset);
  }

  private static void putLong64LE(byte[] input, int offset, long value) {
    LittleEndianBytes.putLong(input, offset, value);
  }

  private long hashEmpty(long seed) {
    return avalancheXXH64(seed ^ DEFAULT_SECRET_BITFLIP_4);
  }

  private <T> long hashLength1To3(HashKey<T> hashKey, int length, long seed) {
    assert length >= 1 && length <= 3;
    int middleOrLast = hashKey.get(length >> 1);
    int first = hashKey.get(0);
    int last = hashKey.get(length - 1);
    long combined = toUnsignedLong((middleOrLast << 24) | (first << 16) | (length << 8) | last);
    long bitflip = DEFAULT_SECRET_BITFLIP_0 + seed;
    return avalancheXXH64(bitflip ^ combined);
  }

  private <T> long hashLength4To8(HashKey<T> hashKey, int length, long seed) {
    assert length >= 4 && length <= 8;
    int inputFirst = readInt32LE(hashKey, 0);
    int inputLast = readInt32LE(hashKey, length - 4);
    long combined = toUnsignedLong(inputLast) | (toUnsignedLong(inputFirst) << 32);
    long modifiedSeed = seed ^ (Long.reverseBytes(seed & MASK32));
    long bitflip = DEFAULT_SECRET_BITFLIP_1 - modifiedSeed;
    long value = combined ^ bitflip;
    return rrmxmx(value, length);
  }

  private <T> long hashLength9To16(HashKey<T> hashKey, int length, long seed) {
    assert length >= 9 && length <= 16;
    long inputFirst = readLong64LE(hashKey, 0);
    long inputLast = readLong64LE(hashKey, length - 8);
    long low = (DEFAULT_SECRET_BITFLIP_2 + seed) ^ inputFirst;
    long high = (DEFAULT_SECRET_BITFLIP_3 - seed) ^ inputLast;
    long value = length + Long.reverseBytes(low) + high + mul128AndFold64(low, high);
    return avalanche(value);
  }

  private <T> long hashSmall(HashKey<T> hashKey, int length, long seed) {
    if (length > 8) {
      return hashLength9To16(hashKey, length, seed);
    } else if (length > 3) {
      return hashLength4To8(hashKey, length, seed);
    } else if (length > 0) {
      return hashLength1To3(hashKey, length, seed);
    }

    return hashEmpty(seed);
  }

  private <T> long hashLength17To128(HashKey<T> hashKey, int length, long seed) {
    assert length >= 17 && length <= 128;
    long acc = length * PRIME64_1;
    switch ((length - 1) / 32) {
      case 3:
        acc += mix16(hashKey, 48, DEFAULT_SECRET_LONG_12, DEFAULT_SECRET_LONG_13, seed);
        acc += mix16(hashKey, length - 64, DEFAULT_SECRET_LONG_14, DEFAULT_SECRET_LONG_15, seed);
      case 2:
        acc += mix16(hashKey, 32, DEFAULT_SECRET_LONG_8, DEFAULT_SECRET_LONG_9, seed);
        acc += mix16(hashKey, length - 48, DEFAULT_SECRET_LONG_10, DEFAULT_SECRET_LONG_11, seed);
      case 1:
        acc += mix16(hashKey, 16, DEFAULT_SECRET_LONG_4, DEFAULT_SECRET_LONG_5, seed);
        acc += mix16(hashKey, length - 32, DEFAULT_SECRET_LONG_6, DEFAULT_SECRET_LONG_7, seed);
      case 0:
        acc += mix16(hashKey, 0, DEFAULT_SECRET_LONG_0, DEFAULT_SECRET_LONG_1, seed);
        acc += mix16(hashKey, length - 16, DEFAULT_SECRET_LONG_2, DEFAULT_SECRET_LONG_3, seed);
        break;
    }

    return avalanche(acc);
  }

  private <T> long hashLength129To240(HashKey<T> hashKey, int length, long seed) {
    assert length >= 129 && length <= 240;
    long acc = length * PRIME64_1;

    acc += mix16(hashKey, 16 * 0, DEFAULT_SECRET_LONG_0, DEFAULT_SECRET_LONG_1, seed);
    acc += mix16(hashKey, 16 * 1, DEFAULT_SECRET_LONG_2, DEFAULT_SECRET_LONG_3, seed);
    acc += mix16(hashKey, 16 * 2, DEFAULT_SECRET_LONG_4, DEFAULT_SECRET_LONG_5, seed);
    acc += mix16(hashKey, 16 * 3, DEFAULT_SECRET_LONG_6, DEFAULT_SECRET_LONG_7, seed);
    acc += mix16(hashKey, 16 * 4, DEFAULT_SECRET_LONG_8, DEFAULT_SECRET_LONG_9, seed);
    acc += mix16(hashKey, 16 * 5, DEFAULT_SECRET_LONG_10, DEFAULT_SECRET_LONG_11, seed);
    acc += mix16(hashKey, 16 * 6, DEFAULT_SECRET_LONG_12, DEFAULT_SECRET_LONG_13, seed);
    acc += mix16(hashKey, 16 * 7, DEFAULT_SECRET_LONG_14, DEFAULT_SECRET_LONG_15, seed);

    acc = avalanche(acc);

    switch ((length - 128) >> 4) {
      case 7:
        acc += mix16(hashKey, 16 * 14, DEFAULT_SECRET_3_LONG_12, DEFAULT_SECRET_3_LONG_13, seed);
      case 6:
        acc += mix16(hashKey, 16 * 13, DEFAULT_SECRET_3_LONG_10, DEFAULT_SECRET_3_LONG_11, seed);
      case 5:
        acc += mix16(hashKey, 16 * 12, DEFAULT_SECRET_3_LONG_8, DEFAULT_SECRET_3_LONG_9, seed);
      case 4:
        acc += mix16(hashKey, 16 * 11, DEFAULT_SECRET_3_LONG_6, DEFAULT_SECRET_3_LONG_7, seed);
      case 3:
        acc += mix16(hashKey, 16 * 10, DEFAULT_SECRET_3_LONG_4, DEFAULT_SECRET_3_LONG_5, seed);
      case 2:
        acc += mix16(hashKey, 16 * 9, DEFAULT_SECRET_3_LONG_2, DEFAULT_SECRET_3_LONG_3, seed);
      case 1:
        acc += mix16(hashKey, 16 * 8, DEFAULT_SECRET_3_LONG_0, DEFAULT_SECRET_3_LONG_1, seed);
      case 0:
        acc += mix16(hashKey, length - 16, DEFAULT_SECRET_LONG_119, DEFAULT_SECRET_LONG_127, seed);
        break;
    }

    return avalanche(acc);
  }

  private <T> long hashMedium(HashKey<T> hashKey, int length, long seed) {
    if (length > 128) {
      return hashLength129To240(hashKey, length, seed);
    } else {
      return hashLength17To128(hashKey, length, seed);
    }
  }

  private <T> long hashLarge(HashKey<T> hashKey, int length, long seed) {
    // Hot path for large inputs.
    // This code is intentionally fully inlined to guarantee zero allocations and keep the inner
    // loop JIT-friendly; no temporary arrays/objects.
    long acc0 = PRIME32_3;
    long acc1 = PRIME64_1;
    long acc2 = PRIME64_2;
    long acc3 = PRIME64_3;
    long acc4 = PRIME64_4;
    long acc5 = PRIME32_2;
    long acc6 = PRIME64_5;
    long acc7 = PRIME32_1;

    byte[] secret = initSecret(seed);
    final int blockSize = 1024;
    final int numberOfBlocks = (length - 1) / blockSize;
    final int numberOfStripesPerBlock = (secret.length - 64) / 8;
    for (int b = 0; b < numberOfBlocks; b++) {
      // per block round accumulation
      for (int s = 0; s < numberOfStripesPerBlock; s++) {
        final int stripeOffset = (b * blockSize) + (s * 64);
        final int secretOffset = s * 8;

        long lane0 = readLong64LE(hashKey, stripeOffset);
        long secret0 = readLong64LE(secret, secretOffset);
        long value0 = lane0 ^ secret0;
        acc1 += lane0;
        acc0 += (value0 & MASK32) * (value0 >>> 32);

        long lane1 = readLong64LE(hashKey, stripeOffset + 8);
        long secret1 = readLong64LE(secret, secretOffset + 8);
        long value1 = lane1 ^ secret1;
        acc0 += lane1;
        acc1 += (value1 & MASK32) * (value1 >>> 32);

        long lane2 = readLong64LE(hashKey, stripeOffset + 16);
        long secret2 = readLong64LE(secret, secretOffset + 16);
        long value2 = lane2 ^ secret2;
        acc3 += lane2;
        acc2 += (value2 & MASK32) * (value2 >>> 32);

        long lane3 = readLong64LE(hashKey, stripeOffset + 24);
        long secret3 = readLong64LE(secret, secretOffset + 24);
        long value3 = lane3 ^ secret3;
        acc2 += lane3;
        acc3 += (value3 & MASK32) * (value3 >>> 32);

        long lane4 = readLong64LE(hashKey, stripeOffset + 32);
        long secret4 = readLong64LE(secret, secretOffset + 32);
        long value4 = lane4 ^ secret4;
        acc5 += lane4;
        acc4 += (value4 & MASK32) * (value4 >>> 32);

        long lane5 = readLong64LE(hashKey, stripeOffset + 40);
        long secret5 = readLong64LE(secret, secretOffset + 40);
        long value5 = lane5 ^ secret5;
        acc4 += lane5;
        acc5 += (value5 & MASK32) * (value5 >>> 32);

        long lane6 = readLong64LE(hashKey, stripeOffset + 48);
        long secret6 = readLong64LE(secret, secretOffset + 48);
        long value6 = lane6 ^ secret6;
        acc7 += lane6;
        acc6 += (value6 & MASK32) * (value6 >>> 32);

        long lane7 = readLong64LE(hashKey, stripeOffset + 56);
        long secret7 = readLong64LE(secret, secretOffset + 56);
        long value7 = lane7 ^ secret7;
        acc6 += lane7;
        acc7 += (value7 & MASK32) * (value7 >>> 32);
      }

      // per block scramble
      acc0 = (acc0 ^ (acc0 >>> 47) ^ readLong64LE(secret, (secret.length - 64))) * PRIME32_1;
      acc1 = (acc1 ^ (acc1 >>> 47) ^ readLong64LE(secret, (secret.length - 56))) * PRIME32_1;
      acc2 = (acc2 ^ (acc2 >>> 47) ^ readLong64LE(secret, (secret.length - 48))) * PRIME32_1;
      acc3 = (acc3 ^ (acc3 >>> 47) ^ readLong64LE(secret, (secret.length - 40))) * PRIME32_1;
      acc4 = (acc4 ^ (acc4 >>> 47) ^ readLong64LE(secret, (secret.length - 32))) * PRIME32_1;
      acc5 = (acc5 ^ (acc5 >>> 47) ^ readLong64LE(secret, (secret.length - 24))) * PRIME32_1;
      acc6 = (acc6 ^ (acc6 >>> 47) ^ readLong64LE(secret, (secret.length - 16))) * PRIME32_1;
      acc7 = (acc7 ^ (acc7 >>> 47) ^ readLong64LE(secret, (secret.length - 8))) * PRIME32_1;
    }

    // Handle last partial block
    final int lastBlockSize = length - (numberOfBlocks * blockSize);
    final int lastBlockOffset = numberOfBlocks * blockSize;
    final int numberOfFullStripes = (lastBlockSize - 1) / 64;
    for (int s = 0; s < numberOfFullStripes; s++) {
      final int stripeOffset = lastBlockOffset + (s * 64);
      final int secretOffset = s * 8;

      long lane0 = readLong64LE(hashKey, stripeOffset);
      long secret0 = readLong64LE(secret, secretOffset);
      long value0 = lane0 ^ secret0;
      acc1 += lane0;
      acc0 += (value0 & MASK32) * (value0 >>> 32);

      long lane1 = readLong64LE(hashKey, stripeOffset + 8);
      long secret1 = readLong64LE(secret, secretOffset + 8);
      long value1 = lane1 ^ secret1;
      acc0 += lane1;
      acc1 += (value1 & MASK32) * (value1 >>> 32);

      long lane2 = readLong64LE(hashKey, stripeOffset + 16);
      long secret2 = readLong64LE(secret, secretOffset + 16);
      long value2 = lane2 ^ secret2;
      acc3 += lane2;
      acc2 += (value2 & MASK32) * (value2 >>> 32);

      long lane3 = readLong64LE(hashKey, stripeOffset + 24);
      long secret3 = readLong64LE(secret, secretOffset + 24);
      long value3 = lane3 ^ secret3;
      acc2 += lane3;
      acc3 += (value3 & MASK32) * (value3 >>> 32);

      long lane4 = readLong64LE(hashKey, stripeOffset + 32);
      long secret4 = readLong64LE(secret, secretOffset + 32);
      long value4 = lane4 ^ secret4;
      acc5 += lane4;
      acc4 += (value4 & MASK32) * (value4 >>> 32);

      long lane5 = readLong64LE(hashKey, stripeOffset + 40);
      long secret5 = readLong64LE(secret, secretOffset + 40);
      long value5 = lane5 ^ secret5;
      acc4 += lane5;
      acc5 += (value5 & MASK32) * (value5 >>> 32);

      long lane6 = readLong64LE(hashKey, stripeOffset + 48);
      long secret6 = readLong64LE(secret, secretOffset + 48);
      long value6 = lane6 ^ secret6;
      acc7 += lane6;
      acc6 += (value6 & MASK32) * (value6 >>> 32);

      long lane7 = readLong64LE(hashKey, stripeOffset + 56);
      long secret7 = readLong64LE(secret, secretOffset + 56);
      long value7 = lane7 ^ secret7;
      acc6 += lane7;
      acc7 += (value7 & MASK32) * (value7 >>> 32);
    }

    // Handle last stripe
    final int lastStripeOffset = length - 64;
    final int lastSecretOffset = secret.length - 71;

    long lane0 = readLong64LE(hashKey, lastStripeOffset);
    long secret0 = readLong64LE(secret, lastSecretOffset);
    long value0 = lane0 ^ secret0;
    acc1 += lane0;
    acc0 += (value0 & MASK32) * (value0 >>> 32);

    long lane1 = readLong64LE(hashKey, lastStripeOffset + 8);
    long secret1 = readLong64LE(secret, lastSecretOffset + 8);
    long value1 = lane1 ^ secret1;
    acc0 += lane1;
    acc1 += (value1 & MASK32) * (value1 >>> 32);

    long lane2 = readLong64LE(hashKey, lastStripeOffset + 16);
    long secret2 = readLong64LE(secret, lastSecretOffset + 16);
    long value2 = lane2 ^ secret2;
    acc3 += lane2;
    acc2 += (value2 & MASK32) * (value2 >>> 32);

    long lane3 = readLong64LE(hashKey, lastStripeOffset + 24);
    long secret3 = readLong64LE(secret, lastSecretOffset + 24);
    long value3 = lane3 ^ secret3;
    acc2 += lane3;
    acc3 += (value3 & MASK32) * (value3 >>> 32);

    long lane4 = readLong64LE(hashKey, lastStripeOffset + 32);
    long secret4 = readLong64LE(secret, lastSecretOffset + 32);
    long value4 = lane4 ^ secret4;
    acc5 += lane4;
    acc4 += (value4 & MASK32) * (value4 >>> 32);

    long lane5 = readLong64LE(hashKey, lastStripeOffset + 40);
    long secret5 = readLong64LE(secret, lastSecretOffset + 40);
    long value5 = lane5 ^ secret5;
    acc4 += lane5;
    acc5 += (value5 & MASK32) * (value5 >>> 32);

    long lane6 = readLong64LE(hashKey, lastStripeOffset + 48);
    long secret6 = readLong64LE(secret, lastSecretOffset + 48);
    long value6 = lane6 ^ secret6;
    acc7 += lane6;
    acc6 += (value6 & MASK32) * (value6 >>> 32);

    long lane7 = readLong64LE(hashKey, lastStripeOffset + 56);
    long secret7 = readLong64LE(secret, lastSecretOffset + 56);
    long value7 = lane7 ^ secret7;
    acc6 += lane7;
    acc7 += (value7 & MASK32) * (value7 >>> 32);

    // do final merge
    long result = length * PRIME64_1;
    result += mul128AndFold64(acc0 ^ readLong64LE(secret, 11), acc1 ^ readLong64LE(secret, 11 + 8));
    result += mul128AndFold64(acc2 ^ readLong64LE(secret, 11 + 16),
      acc3 ^ readLong64LE(secret, 11 + 16 + 8));
    result += mul128AndFold64(acc4 ^ readLong64LE(secret, 11 + 32),
      acc5 ^ readLong64LE(secret, 11 + 32 + 8));
    result += mul128AndFold64(acc6 ^ readLong64LE(secret, 11 + 48),
      acc7 ^ readLong64LE(secret, 11 + 48 + 8));

    return avalanche(result);
  }
}
