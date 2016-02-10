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
package org.apache.hadoop.hbase.io.hfile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.util.DataChecksum;

/**
 * Utility methods to compute and validate checksums.
 */
@InterfaceAudience.Private
public class ChecksumUtil {
  public static final Log LOG = LogFactory.getLog(ChecksumUtil.class);

  /** This is used to reserve space in a byte buffer */
  private static byte[] DUMMY_VALUE = new byte[128 * HFileBlock.CHECKSUM_SIZE];

  /**
   * This is used by unit tests to make checksum failures throw an
   * exception instead of returning null. Returning a null value from
   * checksum validation will cause the higher layer to retry that
   * read with hdfs-level checksums. Instead, we would like checksum
   * failures to cause the entire unit test to fail.
   */
  private static boolean generateExceptions = false;

  /**
   * Generates a checksum for all the data in indata. The checksum is
   * written to outdata.
   * @param indata input data stream
   * @param startOffset starting offset in the indata stream from where to
   *                    compute checkums from
   * @param endOffset ending offset in the indata stream upto
   *                   which checksums needs to be computed
   * @param outdata the output buffer where checksum values are written
   * @param outOffset the starting offset in the outdata where the
   *                  checksum values are written
   * @param checksumType type of checksum
   * @param bytesPerChecksum number of bytes per checksum value
   */
  static void generateChecksums(byte[] indata, int startOffset, int endOffset,
    byte[] outdata, int outOffset, ChecksumType checksumType,
    int bytesPerChecksum) throws IOException {

    if (checksumType == ChecksumType.NULL) {
      return; // No checksum for this block.
    }

    DataChecksum checksum = DataChecksum.newDataChecksum(
        checksumType.getDataChecksumType(), bytesPerChecksum);

    checksum.calculateChunkedSums(
       ByteBuffer.wrap(indata, startOffset, endOffset - startOffset),
       ByteBuffer.wrap(outdata, outOffset, outdata.length - outOffset));
  }

  /**
   * Validates that the data in the specified HFileBlock matches the
   * checksum.  Generates the checksum for the data and
   * then validate that it matches the value stored in the header.
   * If there is a checksum mismatch, then return false. Otherwise
   * return true.
   * The header is extracted from the specified HFileBlock while the
   * data-to-be-verified is extracted from 'data'.
   */
  static boolean validateBlockChecksum(Path path, HFileBlock block,
    byte[] data, int hdrSize) throws IOException {

    // If this is an older version of the block that does not have
    // checksums, then return false indicating that checksum verification
    // did not succeed. Actually, this methiod should never be called
    // when the minorVersion is 0, thus this is a defensive check for a
    // cannot-happen case. Since this is a cannot-happen case, it is
    // better to return false to indicate a checksum validation failure.
    if (!block.getHFileContext().isUseHBaseChecksum()) {
      return false;
    }

    // Get a checksum object based on the type of checksum that is
    // set in the HFileBlock header. A ChecksumType.NULL indicates that
    // the caller is not interested in validating checksums, so we
    // always return true.
    ChecksumType cktype = ChecksumType.codeToType(block.getChecksumType());
    if (cktype == ChecksumType.NULL) {
      return true; // No checksum validations needed for this block.
    }

    // read in the stored value of the checksum size from the header.
    int bytesPerChecksum = block.getBytesPerChecksum();

    DataChecksum dataChecksum = DataChecksum.newDataChecksum(
        cktype.getDataChecksumType(), bytesPerChecksum);
    assert dataChecksum != null;
    int sizeWithHeader =  block.getOnDiskDataSizeWithHeader();
    if (LOG.isTraceEnabled()) {
      LOG.info("dataLength=" + data.length
          + ", sizeWithHeader=" + sizeWithHeader
          + ", checksumType=" + cktype.getName()
          + ", file=" + path.toString()
          + ", offset=" + offset
          + ", headerSize=" + hdrSize
          + ", bytesPerChecksum=" + bytesPerChecksum);
    }
    try {
      dataChecksum.verifyChunkedSums(ByteBuffer.wrap(data, 0, sizeWithHeader),
          ByteBuffer.wrap(data, sizeWithHeader, data.length - sizeWithHeader),
                          path.toString(), 0);
    } catch (ChecksumException e) {
      return false;
    }
    return true;  // checksum is valid
  }

  /**
   * Returns the number of bytes needed to store the checksums for
   * a specified data size
   * @param datasize number of bytes of data
   * @param bytesPerChecksum number of bytes in a checksum chunk
   * @return The number of bytes needed to store the checksum values
   */
  static long numBytes(long datasize, int bytesPerChecksum) {
    return numChunks(datasize, bytesPerChecksum) *
                     HFileBlock.CHECKSUM_SIZE;
  }

  /**
   * Returns the number of checksum chunks needed to store the checksums for
   * a specified data size
   * @param datasize number of bytes of data
   * @param bytesPerChecksum number of bytes in a checksum chunk
   * @return The number of checksum chunks
   */
  static long numChunks(long datasize, int bytesPerChecksum) {
    long numChunks = datasize/bytesPerChecksum;
    if (datasize % bytesPerChecksum != 0) {
      numChunks++;
    }
    return numChunks;
  }

  /**
   * Write dummy checksums to the end of the specified bytes array
   * to reserve space for writing checksums later
   * @param baos OutputStream to write dummy checkum values
   * @param numBytes Number of bytes of data for which dummy checksums
   *                 need to be generated
   * @param bytesPerChecksum Number of bytes per checksum value
   */
  static void reserveSpaceForChecksums(ByteArrayOutputStream baos,
    int numBytes, int bytesPerChecksum) throws IOException {
    long numChunks = numChunks(numBytes, bytesPerChecksum);
    long bytesLeft = numChunks * HFileBlock.CHECKSUM_SIZE;
    while (bytesLeft > 0) {
      long count = Math.min(bytesLeft, DUMMY_VALUE.length);
      baos.write(DUMMY_VALUE, 0, (int)count);
      bytesLeft -= count;
    }
  }

  /**
   * Mechanism to throw an exception in case of hbase checksum
   * failure. This is used by unit tests only.
   * @param value Setting this to true will cause hbase checksum
   *              verification failures to generate exceptions.
   */
  public static void generateExceptionForChecksumFailureForTest(boolean value) {
    generateExceptions = value;
  }
}

