/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.hfile;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * This carries the information on some of the meta data about the HFile. This
 * meta data would be used across the HFileWriter/Readers and the HFileBlocks.
 * This would help to add new information to the HFile.
 * This class is not meant to be immutable.
 */
@InterfaceAudience.Private
public class HFileContext implements HeapSize, Cloneable {

  public static final int DEFAULT_BYTES_PER_CHECKSUM = 16 * 1024;
  public static final ChecksumType DEFAULT_CHECKSUM_TYPE = ChecksumType.CRC32;

  /** Whether checksum is enabled or not**/
  private boolean usesHBaseChecksum = true;
  /** Whether mvcc is to be included in the Read/Write**/
  private boolean includesMvcc = true;
  /**Whether tags are to be included in the Read/Write**/
  private boolean includesTags;
  /**Compression algorithm used**/
  private Algorithm compressAlgo = Algorithm.NONE;
  /** Whether tags to be compressed or not**/
  private boolean compressTags;
  /** the checksum type **/
  private ChecksumType checksumType = DEFAULT_CHECKSUM_TYPE;
  /** the number of bytes per checksum value **/
  private int bytesPerChecksum = DEFAULT_BYTES_PER_CHECKSUM;
  /** Number of uncompressed bytes we allow per block. */
  private int blocksize = HConstants.DEFAULT_BLOCKSIZE;
  private DataBlockEncoding encodingOnDisk = DataBlockEncoding.NONE;
  private DataBlockEncoding encodingInCache = DataBlockEncoding.NONE;

  //Empty constructor.  Go with setters
  public HFileContext() {
  }

  public Algorithm getCompression() {
    return compressAlgo;
  }

  public void setCompressAlgo(Algorithm compressAlgo) {
    this.compressAlgo = compressAlgo;
  }

  public boolean shouldUseHBaseChecksum() {
    return usesHBaseChecksum;
  }

  public void setUsesHBaseChecksum(boolean usesHBaseChecksum) {
    this.usesHBaseChecksum = usesHBaseChecksum;
  }

  public boolean shouldIncludeMvcc() {
    return includesMvcc;
  }

  public void setIncludesMvcc(boolean includesMvcc) {
    this.includesMvcc = includesMvcc;
  }

  public boolean shouldIncludeTags() {
    return includesTags;
  }

  public void setIncludesTags(boolean includesTags) {
    this.includesTags = includesTags;
  }

  public boolean shouldCompressTags() {
    return compressTags;
  }

  public void setCompressTags(boolean compressTags) {
    this.compressTags = compressTags;
  }

  public ChecksumType getChecksumType() {
    return checksumType;
  }

  public void setChecksumType(ChecksumType checksumType) {
    this.checksumType = checksumType;
  }

  public int getBytesPerChecksum() {
    return bytesPerChecksum;
  }

  public void setBytesPerChecksum(int bytesPerChecksum) {
    this.bytesPerChecksum = bytesPerChecksum;
  }

  public int getBlocksize() {
    return blocksize;
  }

  public void setBlocksize(int blocksize) {
    this.blocksize = blocksize;
  }

  public DataBlockEncoding getEncodingOnDisk() {
    return encodingOnDisk;
  }

  public void setEncodingOnDisk(DataBlockEncoding encodingOnDisk) {
    this.encodingOnDisk = encodingOnDisk;
  }

  public DataBlockEncoding getEncodingInCache() {
    return encodingInCache;
  }

  public void setEncodingInCache(DataBlockEncoding encodingInCache) {
    this.encodingInCache = encodingInCache;
  }

  /**
   * HeapSize implementation
   * NOTE : The heapsize should be altered as and when new state variable are added
   * @return heap size of the HFileContext
   */
  @Override
  public long heapSize() {
    long size = ClassSize.align(ClassSize.OBJECT +
        // Algorithm reference, encodingondisk, encodingincache, checksumtype
        4 * ClassSize.REFERENCE +
        2 * Bytes.SIZEOF_INT +
        // usesHBaseChecksum, includesMvcc, includesTags and compressTags
        4 * Bytes.SIZEOF_BOOLEAN);
    return size;
  }

  @Override
  public HFileContext clone() {
    HFileContext clonnedCtx = new HFileContext();
    clonnedCtx.usesHBaseChecksum = this.usesHBaseChecksum;
    clonnedCtx.includesMvcc = this.includesMvcc;
    clonnedCtx.includesTags = this.includesTags;
    clonnedCtx.compressAlgo = this.compressAlgo;
    clonnedCtx.compressTags = this.compressTags;
    clonnedCtx.checksumType = this.checksumType;
    clonnedCtx.bytesPerChecksum = this.bytesPerChecksum;
    clonnedCtx.blocksize = this.blocksize;
    clonnedCtx.encodingOnDisk = this.encodingOnDisk;
    clonnedCtx.encodingInCache = this.encodingInCache;
    return clonnedCtx;
  }
}