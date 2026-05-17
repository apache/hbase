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
package org.apache.hadoop.hbase.io.crypto;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import net.openhft.hashing.LongHashFunction;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Digest algorithms for computing partial identity (digest of key metadata). Each value has a bit
 * position (for encoding which algorithms were used) and a fixed digest size.
 */
@InterfaceAudience.Private
public enum DigestAlgorithms {
  /** XXH3 64-bit; 8 bytes. */
  XXH3((byte) 0x01, 8) {

    @Override
    public void digest(byte[] input, int inputOffset, int inputLength, byte[] out, int outOffset) {
      long h = LongHashFunction.xx3().hashBytes(ByteBuffer.wrap(input, inputOffset, inputLength));
      Bytes.putLong(out, outOffset, h);
    }
  },
  /** XXHash64; 8 bytes. */
  XXHASH64((byte) 0x02, 8) {
    @Override
    public void digest(byte[] input, int inputOffset, int inputLength, byte[] out, int outOffset) {
      long h = LongHashFunction.xx().hashBytes(ByteBuffer.wrap(input, inputOffset, inputLength));
      Bytes.putLong(out, outOffset, h);
    }
  },
  /** MD5; 16 bytes. */
  MD5((byte) 0x04, 16) {
    @Override
    public void digest(byte[] input, int inputOffset, int inputLength, byte[] out, int outOffset) {
      try {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(input, inputOffset, inputLength);
        byte[] d = md.digest();
        System.arraycopy(d, 0, out, outOffset, getDigestSizeBytes());
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }
  },
  /** MetroHash64 */
  METRO64((byte) 0x08, 8) {
    @Override
    public void digest(byte[] input, int inputOffset, int inputLength, byte[] out, int outOffset) {
      long h = LongHashFunction.metro().hashBytes(ByteBuffer.wrap(input, inputOffset, inputLength));
      Bytes.putLong(out, outOffset, h);
    }
  },
  /** WYHASH version 3 */
  WYHASH3((byte) 0x10, 8) {
    @Override
    public void digest(byte[] input, int inputOffset, int inputLength, byte[] out, int outOffset) {
      long h = LongHashFunction.wy_3().hashBytes(ByteBuffer.wrap(input, inputOffset, inputLength));
      Bytes.putLong(out, outOffset, h);
    }
  },;

  private final byte bitPosition;
  private final int digestSizeBytes;

  DigestAlgorithms(byte bitPosition, int digestSizeBytes) {
    this.bitPosition = bitPosition;
    this.digestSizeBytes = digestSizeBytes;
  }

  public byte getBitPosition() {
    return bitPosition;
  }

  public int getDigestSizeBytes() {
    return digestSizeBytes;
  }

  /**
   * Compute the digest of the input bytes and write the result into {@code out} at
   * {@code outOffset}. Caller must ensure {@code out} has at least {@link #getDigestSizeBytes()}
   * bytes at {@code outOffset}. Uses {@link Bytes#putLong} for 8-byte digests to avoid allocating a
   * new byte array.
   * @param input       the input bytes
   * @param inputOffset offset into input
   * @param inputLength number of bytes to hash
   * @param out         output buffer
   * @param outOffset   offset into out where digest is written
   */
  public abstract void digest(byte[] input, int inputOffset, int inputLength, byte[] out,
    int outOffset);

  /**
   * Parse a name (case-insensitive) to a DigestAlgo, or null if not recognized.
   */
  public static DigestAlgorithms fromName(String name) {
    if (name == null) {
      return null;
    }
    String n = name.trim().toUpperCase();
    for (DigestAlgorithms a : values()) {
      if (a.name().equals(n)) {
        return a;
      }
    }
    return null;
  }
}
