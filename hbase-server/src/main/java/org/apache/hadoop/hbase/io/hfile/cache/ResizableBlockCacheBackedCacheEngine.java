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

import java.util.Objects;
import org.apache.hadoop.hbase.io.hfile.ResizableBlockCache;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@link CacheEngine} adapter for a legacy {@link ResizableBlockCache}.
 * <p>
 * This adapter preserves the dynamic sizing capability while legacy block caches are being migrated
 * to native {@link CacheEngine} implementations.
 * </p>
 */
@InterfaceAudience.Private
public class ResizableBlockCacheBackedCacheEngine extends BlockCacheBackedCacheEngine
  implements ResizableCacheEngine {

  private final ResizableBlockCache blockCache;

  /**
   * Creates an engine backed by the supplied resizable block cache.
   * @param blockCache legacy resizable block cache
   */
  public ResizableBlockCacheBackedCacheEngine(ResizableBlockCache blockCache) {
    super(Objects.requireNonNull(blockCache, "blockCache must not be null"));
    this.blockCache = blockCache;
  }

  /**
   * Changes the maximum size of the underlying block cache.
   * @param maxSize new maximum cache size in bytes
   */
  @Override
  public void setMaxSize(long maxSize) {
    blockCache.setMaxSize(maxSize);
  }
}
