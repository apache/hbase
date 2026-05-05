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
package org.apache.hadoop.hbase.io.hfile.cache;

import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Context for cache lookup.
 * <p>
 * This object carries read-path cache lookup intent. It mirrors the existing low-allocation
 * BlockCache request parameters while allowing future call sites to pass structured context.
 * </p>
 */
@InterfaceAudience.Private
public final class CacheRequestContext {

  private final boolean caching;
  private final boolean repeat;
  private final boolean updateCacheMetrics;
  private final BlockType blockType;
  private final boolean compaction;
  private final boolean prefetch;

  private CacheRequestContext(Builder builder) {
    this.caching = builder.caching;
    this.repeat = builder.repeat;
    this.updateCacheMetrics = builder.updateCacheMetrics;
    this.blockType = builder.blockType;
    this.compaction = builder.compaction;
    this.prefetch = builder.prefetch;
  }

  /**
   * Creates a new builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns whether caching is enabled for this request.
   * @return true when caching is enabled
   */
  public boolean isCaching() {
    return caching;
  }

  /**
   * Returns whether this is a repeated lookup for the same block.
   * @return true when this is a repeated lookup
   */
  public boolean isRepeat() {
    return repeat;
  }

  /**
   * Returns whether cache metrics should be updated.
   * @return true when metrics should be updated
   */
  public boolean isUpdateCacheMetrics() {
    return updateCacheMetrics;
  }

  /**
   * Returns the expected block type, if known.
   * @return block type, or null if unknown
   */
  public BlockType getBlockType() {
    return blockType;
  }

  /**
   * Returns whether this request is associated with compaction.
   * @return true when compaction-related
   */
  public boolean isCompaction() {
    return compaction;
  }

  /**
   * Returns whether this request is associated with prefetch.
   * @return true when prefetch-related
   */
  public boolean isPrefetch() {
    return prefetch;
  }

  /**
   * Builder for {@link CacheRequestContext}.
   */
  public static final class Builder {

    private boolean caching;
    private boolean repeat;
    private boolean updateCacheMetrics = true;
    private BlockType blockType;
    private boolean compaction;
    private boolean prefetch;

    private Builder() {
    }

    /**
     * Sets whether caching is enabled.
     * @param caching true when caching is enabled
     * @return this builder
     */
    public Builder setCaching(boolean caching) {
      this.caching = caching;
      return this;
    }

    /**
     * Sets repeated-lookup status.
     * @param repeat true when this is a repeated lookup
     * @return this builder
     */
    public Builder setRepeat(boolean repeat) {
      this.repeat = repeat;
      return this;
    }

    /**
     * Sets whether cache metrics should be updated.
     * @param updateCacheMetrics true when metrics should be updated
     * @return this builder
     */
    public Builder setUpdateCacheMetrics(boolean updateCacheMetrics) {
      this.updateCacheMetrics = updateCacheMetrics;
      return this;
    }

    /**
     * Sets the expected block type.
     * @param blockType expected block type
     * @return this builder
     */
    public Builder setBlockType(BlockType blockType) {
      this.blockType = blockType;
      return this;
    }

    /**
     * Sets compaction request status.
     * @param compaction true when compaction-related
     * @return this builder
     */
    public Builder setCompaction(boolean compaction) {
      this.compaction = compaction;
      return this;
    }

    /**
     * Sets prefetch request status.
     * @param prefetch true when prefetch-related
     * @return this builder
     */
    public Builder setPrefetch(boolean prefetch) {
      this.prefetch = prefetch;
      return this;
    }

    /**
     * Builds the context.
     * @return cache request context
     */
    public CacheRequestContext build() {
      return new CacheRequestContext(this);
    }
  }
}
