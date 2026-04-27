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

import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Default read-only view over a {@link CacheEngine}.
 */
@InterfaceAudience.Private
class CacheEngineView  {

  private final CacheEngine engine;

  /** Create a new view over the given cache engine. */
  
  CacheEngineView(CacheEngine engine) {
    this.engine = engine;
  }

  /* Getters for cache engine properties. */
  /**
   * Returns the name of the cache engine.
   * @return  the name of the cache engine
   */
  public String getName() {
    return engine.getName();
  }

  /**
   * Returns the type of the cache engine.
   * @return  the type of the cache engine
   */
  public CacheEngineType getType() {
    return engine.getType();
  }
  
  /** 
   * Returns the maximum size of the cache in bytes.
   * @return  the maximum size of the cache in bytes
   */
  public long getMaxSize() {
    return engine.getMaxSize();
  }

  /** 
   * Returns the current size of the cache in bytes.
   * @return  the current size of the cache in bytes
   */
  public long getCurrentSize() {
    return engine.getCurrentSize();
  }

  /** 
   * Returns the free size of the cache in bytes.
   * @return  the free size of the cache in bytes
   */
  public long getFreeSize() {
    return engine.getFreeSize();
  }

  /** 
   * Returns the number of blocks currently stored in the cache.
   * @return  the number of blocks currently stored in the cache
   */
  public long getBlockCount() {
    return engine.getBlockCount();
  }

  /** 
   * Returns the number of blocks currently stored in the cache.
   * @return  the number of blocks currently stored in the cache
   */
  public boolean canStore(Cacheable block) {
    if (block instanceof HFileBlock) {
      return engine.blockFitsIntoTheCache((HFileBlock) block).orElse(true);
    }
    return true;
  }
}