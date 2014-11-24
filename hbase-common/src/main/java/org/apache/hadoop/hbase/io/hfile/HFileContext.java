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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * This carries the information on some of the meta data about the HFile. This
 * meta data is used across the HFileWriter/Readers and the HFileBlocks.
 * This helps to add new information to the HFile.
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
  private Compression.Algorithm compressAlgo = Compression.Algorithm.NONE;
  /** Whether tags to be compressed or not**/
  private boolean compressTags;
  /** the checksum type **/
  private ChecksumType checksumType = DEFAULT_CHECKSUM_TYPE;
  /** the number of bytes per checksum value **/
  private int bytesPerChecksum = DEFAULT_BYTES_PER_CHECKSUM;
  /** Number of uncompressed bytes we allow per block. */
  private int blocksize = HConstants.DEFAULT_BLOCKSIZE;
  private DataBlockEncoding encoding = DataBlockEncoding.NONE;
  /** Encryption algorithm and key used */
  private Encryption.Context cryptoContext = Encryption.Context.NONE;

  //Empty constructor.  Go with setters
  public HFileContext() {
  }

  /**
   * Copy constructor
   * @param context
   */
  public HFileContext(HFileContext context) {
    this.usesHBaseChecksum = context.usesHBaseChecksum;
    this.includesMvcc = context.includesMvcc;
    this.includesTags = context.includesTags;
    this.compressAlgo = context.compressAlgo;
    this.compressTags = context.compressTags;
    this.checksumType = context.checksumType;
    this.bytesPerChecksum = context.bytesPerChecksum;
    this.blocksize = context.blocksize;
    this.encoding = context.encoding;
    this.cryptoContext = context.cryptoContext;
  }

  public HFileContext(boolean useHBaseChecksum, boolean includesMvcc, boolean includesTags,
      Compression.Algorithm compressAlgo, boolean compressTags, ChecksumType checksumType,
      int bytesPerChecksum, int blockSize, DataBlockEncoding encoding,
      Encryption.Context cryptoContext) {
    this.usesHBaseChecksum = useHBaseChecksum;
    this.includesMvcc =  includesMvcc;
    this.includesTags = includesTags;
    this.compressAlgo = compressAlgo;
    this.compressTags = compressTags;
    this.checksumType = checksumType;
    this.bytesPerChecksum = bytesPerChecksum;
    this.blocksize = blockSize;
    if (encoding != null) {
      this.encoding = encoding;
    }
    this.cryptoContext = cryptoContext;
  }

  /**
   * @return true when on-disk blocks from this file are compressed, and/or encrypted;
   * false otherwise.
   */
  public boolean isCompressedOrEncrypted() {
    Compression.Algorithm compressAlgo = getCompression();
    boolean compressed =
      compressAlgo != null
        && compressAlgo != Compression.Algorithm.NONE;

    Encryption.Context cryptoContext = getEncryptionContext();
    boolean encrypted = cryptoContext != null
      && cryptoContext != Encryption.Context.NONE;

    return compressed || encrypted;
  }

  public Compression.Algorithm getCompression() {
    return compressAlgo;
  }

  public void setCompression(Compression.Algorithm compressAlgo) {
    this.compressAlgo = compressAlgo;
  }

  public boolean isUseHBaseChecksum() {
    return usesHBaseChecksum;
  }

  public boolean isIncludesMvcc() {
    return includesMvcc;
  }

  public void setIncludesMvcc(boolean includesMvcc) {
    this.includesMvcc = includesMvcc;
  }

  public boolean isIncludesTags() {
    return includesTags;
  }

  public void setIncludesTags(boolean includesTags) {
    this.includesTags = includesTags;
  }

  public boolean isCompressTags() {
    return compressTags;
  }

  public void setCompressTags(boolean compressTags) {
    this.compressTags = compressTags;
  }

  public ChecksumType getChecksumType() {
    return checksumType;
  }

  public int getBytesPerChecksum() {
    return bytesPerChecksum;
  }

  public int getBlocksize() {
    return blocksize;
  }

  public DataBlockEncoding getDataBlockEncoding() {
    return encoding;
  }

  public void setDataBlockEncoding(DataBlockEncoding encoding) {
    this.encoding = encoding;
  }

  public Encryption.Context getEncryptionContext() {
    return cryptoContext;
  }

  public void setEncryptionContext(Encryption.Context cryptoContext) {
    this.cryptoContext = cryptoContext;
  }

  /**
   * HeapSize implementation
   * NOTE : The heapsize should be altered as and when new state variable are added
   * @return heap size of the HFileContext
   */
  @Override
  public long heapSize() {
    long size = ClassSize.align(ClassSize.OBJECT +
        // Algorithm reference, encodingon, checksumtype, Encryption.Context reference
        4 * ClassSize.REFERENCE +
        2 * Bytes.SIZEOF_INT +
        // usesHBaseChecksum, includesMvcc, includesTags and compressTags
        4 * Bytes.SIZEOF_BOOLEAN);
    return size;
  }

  @Override
  public HFileContext clone() {
    try {
      return (HFileContext)(super.clone());
    } catch (CloneNotSupportedException e) {
      throw new AssertionError(); // Won't happen
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("HFileContext [");
    sb.append(" usesHBaseChecksum="); sb.append(usesHBaseChecksum);
    sb.append(" checksumType=");      sb.append(checksumType);
    sb.append(" bytesPerChecksum=");  sb.append(bytesPerChecksum);
    sb.append(" blocksize=");         sb.append(blocksize);
    sb.append(" encoding=");          sb.append(encoding);
    sb.append(" includesMvcc=");      sb.append(includesMvcc);
    sb.append(" includesTags=");      sb.append(includesTags);
    sb.append(" compressAlgo=");      sb.append(compressAlgo);
    sb.append(" compressTags=");      sb.append(compressTags);
    sb.append(" cryptoContext=[ ");   sb.append(cryptoContext);      sb.append(" ]");
    sb.append(" ]");
    return sb.toString();
  }

}
