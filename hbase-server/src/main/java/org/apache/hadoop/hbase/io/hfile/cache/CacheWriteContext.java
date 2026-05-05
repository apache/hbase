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

import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Context for cache insertion.
 * <p>
 * This object carries insertion intent from read-miss population, write path population, prefetch,
 * compaction, or promotion. It is intentionally small and immutable.
 * </p>
 */
@InterfaceAudience.Private
public final class CacheWriteContext {

  private final boolean inMemory;
  private final boolean waitWhenCache;
  private final boolean cacheCompressed;
  private final boolean cacheOnWrite;
  private final BlockCategory blockCategory;
  private final CacheWriteSource source;

  private CacheWriteContext(Builder builder) {
    this.inMemory = builder.inMemory;
    this.waitWhenCache = builder.waitWhenCache;
    this.cacheCompressed = builder.cacheCompressed;
    this.cacheOnWrite = builder.cacheOnWrite;
    this.blockCategory = builder.blockCategory;
    this.source = builder.source;
  }

  /**
   * Creates a new builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns whether this block should receive in-memory treatment.
   * @return true when in-memory treatment is requested
   */
  public boolean isInMemory() {
    return inMemory;
  }

  /**
   * Returns whether insertion should wait for asynchronous cache acceptance.
   * @return true when wait is requested
   */
  public boolean isWaitWhenCache() {
    return waitWhenCache;
  }

  /**
   * Returns whether compressed caching is requested.
   * @return true when compressed caching is requested
   */
  public boolean isCacheCompressed() {
    return cacheCompressed;
  }

  /**
   * Returns whether this insertion comes from cache-on-write behavior.
   * @return true when cache-on-write is enabled
   */
  public boolean isCacheOnWrite() {
    return cacheOnWrite;
  }

  /**
   * Returns the block category, if known.
   * @return block category, or null if unknown
   */
  public BlockCategory getBlockCategory() {
    return blockCategory;
  }

  /**
   * Returns the source of this cache write.
   * @return cache write source
   */
  public CacheWriteSource getSource() {
    return source;
  }

  /**
   * Builder for {@link CacheWriteContext}.
   */
  public static final class Builder {

    private boolean inMemory;
    private boolean waitWhenCache;
    private boolean cacheCompressed;
    private boolean cacheOnWrite;
    private BlockCategory blockCategory;
    private CacheWriteSource source = CacheWriteSource.READ_MISS;

    private Builder() {
    }

    /**
     * Sets in-memory treatment.
     * @param inMemory true when in-memory treatment is requested
     * @return this builder
     */
    public Builder setInMemory(boolean inMemory) {
      this.inMemory = inMemory;
      return this;
    }

    /**
     * Sets wait-on-cache behavior.
     * @param waitWhenCache true when insertion should wait
     * @return this builder
     */
    public Builder setWaitWhenCache(boolean waitWhenCache) {
      this.waitWhenCache = waitWhenCache;
      return this;
    }

    /**
     * Sets compressed cache preference.
     * @param cacheCompressed true when compressed caching is requested
     * @return this builder
     */
    public Builder setCacheCompressed(boolean cacheCompressed) {
      this.cacheCompressed = cacheCompressed;
      return this;
    }

    /**
     * Sets cache-on-write status.
     * @param cacheOnWrite true when cache-on-write is enabled
     * @return this builder
     */
    public Builder setCacheOnWrite(boolean cacheOnWrite) {
      this.cacheOnWrite = cacheOnWrite;
      return this;
    }

    /**
     * Sets the block category.
     * @param blockCategory block category
     * @return this builder
     */
    public Builder setBlockCategory(BlockCategory blockCategory) {
      this.blockCategory = blockCategory;
      return this;
    }

    /**
     * Sets the cache write source.
     * @param source cache write source
     * @return this builder
     */
    public Builder setSource(CacheWriteSource source) {
      this.source = source;
      return this;
    }

    /**
     * Builds the context.
     * @return cache write context
     */
    public CacheWriteContext build() {
      return new CacheWriteContext(this);
    }
  }
}
