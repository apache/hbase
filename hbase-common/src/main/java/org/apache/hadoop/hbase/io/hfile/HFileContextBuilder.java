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
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.ChecksumType;

/**
 * A builder that helps in building up the HFileContext 
 */
@InterfaceAudience.Private
public class HFileContextBuilder {

  public static final int DEFAULT_BYTES_PER_CHECKSUM = 16 * 1024;
  public static final ChecksumType DEFAULT_CHECKSUM_TYPE = ChecksumType.CRC32;

  /** Whether checksum is enabled or not **/
  private boolean usesHBaseChecksum = true;
  /** Whether mvcc is to be included in the Read/Write **/
  private boolean includesMvcc = true;
  /** Whether tags are to be included in the Read/Write **/
  private boolean includesTags;
  /** Compression algorithm used **/
  private Algorithm compression = Algorithm.NONE;
  /** Whether tags to be compressed or not **/
  private boolean compressTags = false;
  /** the checksum type **/
  private ChecksumType checksumType = DEFAULT_CHECKSUM_TYPE;
  /** the number of bytes per checksum value **/
  private int bytesPerChecksum = DEFAULT_BYTES_PER_CHECKSUM;
  /** Number of uncompressed bytes we allow per block. */
  private int blocksize = HConstants.DEFAULT_BLOCKSIZE;
  private DataBlockEncoding encoding = DataBlockEncoding.NONE;
  /** Crypto context */
  private Encryption.Context cryptoContext = Encryption.Context.NONE;

  public HFileContextBuilder withHBaseCheckSum(boolean useHBaseCheckSum) {
    this.usesHBaseChecksum = useHBaseCheckSum;
    return this;
  }

  public HFileContextBuilder withIncludesMvcc(boolean includesMvcc) {
    this.includesMvcc = includesMvcc;
    return this;
  }

  public HFileContextBuilder withIncludesTags(boolean includesTags) {
    this.includesTags = includesTags;
    return this;
  }

  public HFileContextBuilder withCompression(Algorithm compression) {
    this.compression = compression;
    return this;
  }

  public HFileContextBuilder withCompressTags(boolean compressTags) {
    this.compressTags = compressTags;
    return this;
  }

  public HFileContextBuilder withChecksumType(ChecksumType checkSumType) {
    this.checksumType = checkSumType;
    return this;
  }

  public HFileContextBuilder withBytesPerCheckSum(int bytesPerChecksum) {
    this.bytesPerChecksum = bytesPerChecksum;
    return this;
  }

  public HFileContextBuilder withBlockSize(int blockSize) {
    this.blocksize = blockSize;
    return this;
  }

  public HFileContextBuilder withDataBlockEncoding(DataBlockEncoding encoding) {
    this.encoding = encoding;
    return this;
  }

  public HFileContextBuilder withEncryptionContext(Encryption.Context cryptoContext) {
    this.cryptoContext = cryptoContext;
    return this;
  }

  public HFileContext build() {
    return new HFileContext(usesHBaseChecksum, includesMvcc, includesTags, compression,
      compressTags, checksumType, bytesPerChecksum, blocksize, encoding, cryptoContext);
  }
}
