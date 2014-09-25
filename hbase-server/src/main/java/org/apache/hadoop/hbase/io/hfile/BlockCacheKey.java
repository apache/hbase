/**
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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * Cache Key for use with implementations of {@link BlockCache}
 */
@InterfaceAudience.Private
public class BlockCacheKey implements HeapSize, java.io.Serializable {
  private static final long serialVersionUID = -5199992013113130534L;
  private final String hfileName;
  private final long offset;
  private final DataBlockEncoding encoding;

  public BlockCacheKey(String file, long offset, DataBlockEncoding encoding,
      BlockType blockType) {
    this.hfileName = file;
    this.offset = offset;
    // We add encoding to the cache key only for data blocks. If the block type
    // is unknown (this should never be the case in production), we just use
    // the provided encoding, because it might be a data block.
    this.encoding = (encoding != null && (blockType == null
      || blockType.isData())) ? encoding : DataBlockEncoding.NONE;
  }

  /**
   * Construct a new BlockCacheKey
   * @param file The name of the HFile this block belongs to.
   * @param offset Offset of the block into the file
   */
  public BlockCacheKey(String file, long offset) {
    this(file, offset, DataBlockEncoding.NONE, null);
  }

  @Override
  public int hashCode() {
    return hfileName.hashCode() * 127 + (int) (offset ^ (offset >>> 32)) +
        encoding.ordinal() * 17;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof BlockCacheKey) {
      BlockCacheKey k = (BlockCacheKey) o;
      return offset == k.offset && encoding == k.encoding
          && (hfileName == null ? k.hfileName == null : hfileName
              .equals(k.hfileName));
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return hfileName + "_" + offset
        + (encoding == DataBlockEncoding.NONE ? "" : "_" + encoding);
  }

  /**
   * Strings have two bytes per character due to default Java Unicode encoding
   * (hence length times 2).
   */
  @Override
  public long heapSize() {
    return ClassSize.align(ClassSize.OBJECT + 2 * hfileName.length() +
        Bytes.SIZEOF_LONG + 2 * ClassSize.REFERENCE);
  }

  // can't avoid this unfortunately
  /**
   * @return The hfileName portion of this cache key
   */
  public String getHfileName() {
    return hfileName;
  }

  public DataBlockEncoding getDataBlockEncoding() {
    return encoding;
  }

  public long getOffset() {
    return offset;
  }
}
