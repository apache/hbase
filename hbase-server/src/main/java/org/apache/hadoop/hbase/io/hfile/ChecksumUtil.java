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
import java.util.zip.Checksum;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.util.DataChecksum;

/**
 * Utility methods to compute and validate checksums.
 */
@InterfaceAudience.Private
public class ChecksumUtil {

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
  static void generateChecksums(byte[] indata,
    int startOffset, int endOffset, 
    byte[] outdata, int outOffset,
    ChecksumType checksumType,
    int bytesPerChecksum) throws IOException {

    if (checksumType == ChecksumType.NULL) {
      return; // No checkums for this block.
    }

    Checksum checksum = checksumType.getChecksumObject();
    int bytesLeft = endOffset - startOffset;
    int chunkNum = 0;

    while (bytesLeft > 0) {
      // generate the checksum for one chunk
      checksum.reset();
      int count = Math.min(bytesLeft, bytesPerChecksum);
      checksum.update(indata, startOffset, count);

      // write the checksum value to the output buffer.
      int cksumValue = (int)checksum.getValue();
      outOffset = Bytes.putInt(outdata, outOffset, cksumValue);
      chunkNum++;
      startOffset += count;
      bytesLeft -= count;
    }
  }

  /**
   * Validates that the data in the specified HFileBlock matches the checksum. Generates the
   * checksums for the data and then validate that it matches those stored in the end of the data.
   * @param buffer Contains the data in following order: HFileBlock header, data, checksums.
   * @param path Path of the HFile to which the {@code data} belongs. Only used for logging.
   * @param offset offset of the data being validated. Only used for logging.
   * @param hdrSize Size of the block header in {@code data}. Only used for logging.
   * @return True if checksum matches, else false.
   */
  static boolean validateChecksum(ByteBuffer buffer, Path path, long offset, int hdrSize)
      throws IOException {
    // A ChecksumType.NULL indicates that the caller is not interested in validating checksums,
    // so we always return true.
    ChecksumType cktype =
        ChecksumType.codeToType(buffer.get(HFileBlock.Header.CHECKSUM_TYPE_INDEX));
    if (cktype == ChecksumType.NULL) {
      return true; // No checkums validations needed for this block.
    }
    // read in the stored value of the checksum size from the header.
    int bytesPerChecksum = buffer.getInt(HFileBlock.Header.BYTES_PER_CHECKSUM_INDEX);
    int onDiskDataSizeWithHeader =
        buffer.getInt(HFileBlock.Header.ON_DISK_DATA_SIZE_WITH_HEADER_INDEX);

    if (HFile.LOG.isTraceEnabled()) {
      HFile.LOG.info("dataLength=" + buffer.capacity()
          + ", sizeWithHeader=" + onDiskDataSizeWithHeader
          + ", checksumType=" + cktype.getName()
          + ", file=" + path.toString()
          + ", offset=" + offset
          + ", headerSize=" + hdrSize
          + ", bytesPerChecksum=" + bytesPerChecksum);
    }
    // bytesPerChecksum is always larger than the size of the header
    if (bytesPerChecksum < hdrSize) {
      String msg = "Unsupported value of bytesPerChecksum. " +
                   " Minimum is " + hdrSize + 
                   " but the configured value is " + bytesPerChecksum;
      HFile.LOG.warn(msg);
      return false;   // cannot happen case, unable to verify checksum
    }
    byte[] data;
    if (buffer.hasArray()) {
      data = buffer.array();
    } else {
      data = ByteBufferUtils.toBytes(buffer, 0);
    }

    Checksum checksumObject = cktype.getChecksumObject();
    checksumObject.reset();
    // Extract the header and compute checksum for the header.
    checksumObject.update(data, 0, hdrSize);

    int off = hdrSize;
    int consumed = hdrSize;
    int cksumOffset = onDiskDataSizeWithHeader;
    int bytesLeft = cksumOffset - off;

    // validate each chunk
    while (bytesLeft > 0) {
      int thisChunkSize = bytesPerChecksum - consumed;
      int count = Math.min(bytesLeft, thisChunkSize);
      checksumObject.update(data, off, count);

      int storedChecksum = Bytes.toInt(data, cksumOffset);
      if (storedChecksum != (int)checksumObject.getValue()) {
        String msg = "File " + path +
                     " Stored checksum value of " + storedChecksum +
                     " at offset " + cksumOffset +
                     " does not match computed checksum " +
                     checksumObject.getValue() +
                     ", total data size " + data.length +
                     " Checksum data range offset " + off + " len " + count +
                     HFileBlock.toStringHeader(buffer);
        HFile.LOG.warn(msg);
        if (generateExceptions) {
          throw new IOException(msg); // this is only for unit tests
        } else {
          return false;               // checksum validation failure
        }
      }
      cksumOffset += HFileBlock.CHECKSUM_SIZE;
      bytesLeft -= count; 
      off += count;
      consumed = 0;
      checksumObject.reset();
    }
    return true; // checksum is valid
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

