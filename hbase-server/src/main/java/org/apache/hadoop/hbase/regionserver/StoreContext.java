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
package org.apache.hadoop.hbase.regionserver;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.function.Supplier;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This carries the immutable information and references on some of the meta data about the HStore.
 * This meta data can be used across the HFileWriter/Readers and other HStore consumers without the
 * need of passing around the complete store.
 */
@InterfaceAudience.Private
public final class StoreContext implements HeapSize {
  public static final long FIXED_OVERHEAD = ClassSize.estimateBase(HStore.class, false);

  private final int blockSize;
  private final Encryption.Context encryptionContext;
  private final CacheConfig cacheConf;
  private final HRegionFileSystem regionFileSystem;
  private final CellComparator comparator;
  private final BloomType bloomFilterType;
  private final Supplier<Collection<HStoreFile>> compactedFilesSupplier;
  private final Supplier<InetSocketAddress[]> favoredNodesSupplier;
  private final ColumnFamilyDescriptor family;
  private final Path familyStoreDirectoryPath;
  private final RegionCoprocessorHost coprocessorHost;

  private StoreContext(Builder builder) {
    this.blockSize = builder.blockSize;
    this.encryptionContext = builder.encryptionContext;
    this.cacheConf = builder.cacheConf;
    this.regionFileSystem = builder.regionFileSystem;
    this.comparator = builder.comparator;
    this.bloomFilterType = builder.bloomFilterType;
    this.compactedFilesSupplier = builder.compactedFilesSupplier;
    this.favoredNodesSupplier = builder.favoredNodesSupplier;
    this.family = builder.family;
    this.familyStoreDirectoryPath = builder.familyStoreDirectoryPath;
    this.coprocessorHost = builder.coprocessorHost;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public Encryption.Context getEncryptionContext() {
    return encryptionContext;
  }

  public CacheConfig getCacheConf() {
    return cacheConf;
  }

  public HRegionFileSystem getRegionFileSystem() {
    return regionFileSystem;
  }

  public CellComparator getComparator() {
    return comparator;
  }

  public BloomType getBloomFilterType() {
    return bloomFilterType;
  }

  public Supplier<Collection<HStoreFile>> getCompactedFilesSupplier() {
    return compactedFilesSupplier;
  }

  public InetSocketAddress[] getFavoredNodes() {
    return favoredNodesSupplier.get();
  }

  public ColumnFamilyDescriptor getFamily() {
    return family;
  }

  public Path getFamilyStoreDirectoryPath() {
    return familyStoreDirectoryPath;
  }

  public RegionCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  public static Builder getBuilder() {
    return new Builder();
  }

  @Override
  public long heapSize() {
    return FIXED_OVERHEAD;
  }

  public static class Builder {
    private int blockSize;
    private Encryption.Context encryptionContext;
    private CacheConfig cacheConf;
    private HRegionFileSystem regionFileSystem;
    private CellComparator comparator;
    private BloomType bloomFilterType;
    private Supplier<Collection<HStoreFile>> compactedFilesSupplier;
    private Supplier<InetSocketAddress[]> favoredNodesSupplier;
    private ColumnFamilyDescriptor family;
    private Path familyStoreDirectoryPath;
    private RegionCoprocessorHost coprocessorHost;

    public Builder withBlockSize(int blockSize) {
      this.blockSize = blockSize;
      return this;
    }

    public Builder withEncryptionContext(Encryption.Context encryptionContext) {
      this.encryptionContext = encryptionContext;
      return this;
    }

    public Builder withCacheConfig(CacheConfig cacheConf) {
      this.cacheConf = cacheConf;
      return this;
    }

    public Builder withRegionFileSystem(HRegionFileSystem regionFileSystem) {
      this.regionFileSystem = regionFileSystem;
      return this;
    }

    public Builder withCellComparator(CellComparator comparator) {
      this.comparator = comparator;
      return this;
    }

    public Builder withBloomType(BloomType bloomFilterType) {
      this.bloomFilterType = bloomFilterType;
      return this;
    }

    public Builder withCompactedFilesSupplier(Supplier<Collection<HStoreFile>>
        compactedFilesSupplier) {
      this.compactedFilesSupplier = compactedFilesSupplier;
      return this;
    }

    public Builder withFavoredNodesSupplier(Supplier<InetSocketAddress[]> favoredNodesSupplier) {
      this.favoredNodesSupplier = favoredNodesSupplier;
      return this;
    }

    public Builder withColumnFamilyDescriptor(ColumnFamilyDescriptor family) {
      this.family = family;
      return this;
    }

    public Builder withFamilyStoreDirectoryPath(Path familyStoreDirectoryPath) {
      this.familyStoreDirectoryPath = familyStoreDirectoryPath;
      return this;
    }

    public Builder withRegionCoprocessorHost(RegionCoprocessorHost coprocessorHost) {
      this.coprocessorHost = coprocessorHost;
      return this;
    }

    public StoreContext build() {
      return new StoreContext(this);
    }
  }

}
