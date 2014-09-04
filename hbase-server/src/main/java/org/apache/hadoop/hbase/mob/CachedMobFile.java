/**
 *
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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;

/**
 * Cached mob file.
 */
@InterfaceAudience.Private
public class CachedMobFile extends MobFile implements Comparable<CachedMobFile> {

  private long accessCount;
  private AtomicLong referenceCount = new AtomicLong(0);

  public CachedMobFile(StoreFile sf) {
    super(sf);
  }

  public static CachedMobFile create(FileSystem fs, Path path, Configuration conf,
      CacheConfig cacheConf) throws IOException {
    StoreFile sf = new StoreFile(fs, path, conf, cacheConf, BloomType.NONE);
    return new CachedMobFile(sf);
  }

  public void access(long accessCount) {
    this.accessCount = accessCount;
  }

  public int compareTo(CachedMobFile that) {
    if (this.accessCount == that.accessCount) return 0;
    return this.accessCount < that.accessCount ? 1 : -1;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof CachedMobFile)) {
      return false;
    }
    return compareTo((CachedMobFile) obj) == 0;
  }

  @Override
  public int hashCode() {
    return (int)(accessCount ^ (accessCount >>> 32));
  }

  /**
   * Opens the mob file if it's not opened yet and increases the reference.
   * It's not thread-safe. Use MobFileCache.openFile() instead.
   * The reader of the mob file is just opened when it's not opened no matter how many times
   * this open() method is invoked.
   * The reference is a counter that how many times this reader is referenced. When the
   * reference is 0, this reader is closed.
   */
  @Override
  public void open() throws IOException {
    super.open();
    referenceCount.incrementAndGet();
  }

  /**
   * Decreases the reference of the underlying reader for the mob file.
   * It's not thread-safe. Use MobFileCache.closeFile() instead.
   * This underlying reader isn't closed until the reference is 0.
   */
  @Override
  public void close() throws IOException {
    long refs = referenceCount.decrementAndGet();
    if (refs == 0) {
      super.close();
    }
  }

  /**
   * Gets the reference of the current mob file.
   * Internal usage, currently it's for testing.
   * @return The reference of the current mob file.
   */
  public long getReferenceCount() {
    return this.referenceCount.longValue();
  }
}