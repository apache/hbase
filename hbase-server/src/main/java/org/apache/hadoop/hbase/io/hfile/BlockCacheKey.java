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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.bucket.FilePathStringPool;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Cache Key for use with implementations of {@link BlockCache}
 */
@InterfaceAudience.Private
public class BlockCacheKey implements HeapSize, java.io.Serializable {
  private static final long serialVersionUID = -5199992013113130535L; // Changed due to format
                                                                      // change

  // New compressed format using integer file ID (when codec is available)
  private final int hfileNameId;

  // Reference to codec for decoding file IDs back to names
  private final FilePathStringPool stringPool;

  private final int regionId;

  private final int cfId;

  private final long offset;

  private BlockType blockType;

  private final boolean isPrimaryReplicaBlock;

  private final boolean archived;

  /**
   * Constructs a new BlockCacheKey with the file name and offset only. To be used for cache lookups
   * only, DO NOT use this for creating keys when inserting into the cache. Use either the
   * overriding constructors with the path parameter or the region and cf parameters, otherwise,
   * region cache metrics won't be recorded properly.
   * @param hfileName The name of the HFile this block belongs to.
   * @param offset    Offset of the block into the file
   */
  public BlockCacheKey(String hfileName, long offset) {
    this(hfileName, null, null, offset, true, BlockType.DATA, false);
  }

  /**
   * Constructs a new BlockCacheKey with the file name, offset, replica and type only. To be used
   * for cache lookups only, DO NOT use this for creating keys when inserting into the cache. Use
   * either the overriding constructors with the path parameter or the region and cf parameters,
   * otherwise, region cache metrics won't be recorded properly.
   * @param hfileName        The name of the HFile this block belongs to.
   * @param offset           Offset of the block into the file
   * @param isPrimaryReplica Whether this is from primary replica
   * @param blockType        Type of block
   */
  public BlockCacheKey(String hfileName, long offset, boolean isPrimaryReplica,
    BlockType blockType) {
    this(hfileName, null, null, offset, isPrimaryReplica, blockType, false);
  }

  /**
   * Construct a new BlockCacheKey, with file, column family and region information. This should be
   * used when inserting keys into the cache, so that region cache metrics are recorded properly.
   * @param hfileName        The name of the HFile this block belongs to.
   * @param cfName           The column family name
   * @param regionName       The region name
   * @param offset           Offset of the block into the file
   * @param isPrimaryReplica Whether this is from primary replica
   * @param blockType        Type of block
   */
  public BlockCacheKey(String hfileName, String cfName, String regionName, long offset,
    boolean isPrimaryReplica, BlockType blockType, boolean archived) {
    this.isPrimaryReplicaBlock = isPrimaryReplica;
    this.offset = offset;
    this.blockType = blockType;
    this.stringPool = FilePathStringPool.getInstance();
    // Use string pool for file, region and cf values
    this.hfileNameId = stringPool.encode(hfileName);
    this.regionId = (regionName != null) ? stringPool.encode(regionName) : -1;
    this.cfId = (cfName != null) ? stringPool.encode(cfName) : -1;
    this.archived = archived;
  }

  /**
   * Construct a new BlockCacheKey using a file path. File, column family and region information
   * will be extracted from the passed path. This should be used when inserting keys into the cache,
   * so that region cache metrics are recorded properly.
   * @param hfilePath        The path to the HFile
   * @param offset           Offset of the block into the file
   * @param isPrimaryReplica Whether this is from primary replica
   * @param blockType        Type of block
   */
  public BlockCacheKey(Path hfilePath, long offset, boolean isPrimaryReplica, BlockType blockType) {
    this(hfilePath.getName(), hfilePath.getParent().getName(),
      hfilePath.getParent().getParent().getName(), offset, isPrimaryReplica, blockType,
      HFileArchiveUtil.isHFileArchived(hfilePath));
  }

  @Override
  public int hashCode() {
    return hfileNameId * 127 + (int) (offset ^ (offset >>> 32));
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof BlockCacheKey) {
      BlockCacheKey k = (BlockCacheKey) o;
      if (offset != k.offset) {
        return false;
      }
      return getHfileName().equals(k.getHfileName());
    }
    return false;
  }

  @Override
  public String toString() {
    return getHfileName() + '_' + this.offset;
  }

  public static final long FIXED_OVERHEAD = ClassSize.estimateBase(BlockCacheKey.class, false);

  /**
   * With the compressed format using integer file IDs, the heap size is significantly reduced. We
   * now only store a 4-byte integer instead of the full file name string.
   */
  @Override
  public long heapSize() {
    return ClassSize.align(FIXED_OVERHEAD);
  }

  /**
   * Returns the hfileName portion of this cache key.
   * @return The file name
   */
  public String getHfileName() {
    return stringPool.decode(hfileNameId);
  }

  /**
   * Returns the hfileName portion of this cache key.
   * @return The region name
   */
  public String getRegionName() {
    return stringPool.decode(regionId);
  }

  /**
   * Returns the compressed file ID.
   * @return the integer file ID
   */
  public int getRegionId() {
    return regionId;
  }

  /**
   * Returns the column family name portion of this cache key.
   * @return The column family name
   */
  public String getCfName() {
    return stringPool.decode(cfId);
  }

  public boolean isPrimary() {
    return isPrimaryReplicaBlock;
  }

  public long getOffset() {
    return offset;
  }

  public BlockType getBlockType() {
    return blockType;
  }

  public void setBlockType(BlockType blockType) {
    this.blockType = blockType;
  }

  public boolean isArchived() {
    return archived;
  }

}
