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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.MultiTenantHFileWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility functions for region server storage layer.
 */
@InterfaceAudience.Private
public final class StoreUtils {

  private static final Logger LOG = LoggerFactory.getLogger(StoreUtils.class);
  private static final Set<String> MULTI_TENANT_CONF_KEYS =
    Collections.unmodifiableSet(new java.util.HashSet<>(
      java.util.Arrays.asList(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED,
        MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH,
        MultiTenantHFileWriter.TENANT_PREFIX_LENGTH)));

  private StoreUtils() {
  }

  /**
   * Creates a deterministic hash code for store file collection.
   */
  public static OptionalInt getDeterministicRandomSeed(Collection<HStoreFile> files) {
    return files.stream().mapToInt(f -> f.getPath().getName().hashCode()).findFirst();
  }

  /**
   * Determines whether any files in the collection are references.
   * @param files The files.
   */
  public static boolean hasReferences(Collection<HStoreFile> files) {
    // TODO: make sure that we won't pass null here in the future.
    return files != null && files.stream().anyMatch(HStoreFile::isReference);
  }

  /**
   * Gets lowest timestamp from candidate StoreFiles
   */
  public static long getLowestTimestamp(Collection<HStoreFile> candidates) throws IOException {
    long minTs = Long.MAX_VALUE;
    for (HStoreFile storeFile : candidates) {
      minTs = Math.min(minTs, storeFile.getModificationTimestamp());
    }
    return minTs;
  }

  /**
   * Gets the largest file (with reader) out of the list of files.
   * @param candidates The files to choose from.
   * @return The largest file; null if no file has a reader.
   */
  static Optional<HStoreFile> getLargestFile(Collection<HStoreFile> candidates) {
    return candidates.stream().filter(f -> f.getReader() != null)
      .max((f1, f2) -> Long.compare(f1.getReader().length(), f2.getReader().length()));
  }

  /**
   * Return the largest memstoreTS found across all storefiles in the given list. Store files that
   * were created by a mapreduce bulk load are ignored, as they do not correspond to any specific
   * put operation, and thus do not have a memstoreTS associated with them.
   */
  public static OptionalLong getMaxMemStoreTSInList(Collection<HStoreFile> sfs) {
    return sfs.stream().filter(sf -> !sf.isBulkLoadResult()).mapToLong(HStoreFile::getMaxMemStoreTS)
      .max();
  }

  /**
   * Return the highest sequence ID found across all storefiles in the given list.
   */
  public static OptionalLong getMaxSequenceIdInList(Collection<HStoreFile> sfs) {
    return sfs.stream().mapToLong(HStoreFile::getMaxSequenceId).max();
  }

  /**
   * Gets the approximate mid-point of the given file that is optimal for use in splitting it.
   * @param file       the store file
   * @param comparator Comparator used to compare KVs.
   * @return The split point row, or null if splitting is not possible, or reader is null.
   */
  static Optional<byte[]> getFileSplitPoint(HStoreFile file, CellComparator comparator)
    throws IOException {
    StoreFileReader reader = file.getReader();
    if (reader == null) {
      LOG.warn("Storefile " + file + " Reader is null; cannot get split point");
      return Optional.empty();
    }
    // Get first, last, and mid keys. Midkey is the key that starts block
    // in middle of hfile. Has column and timestamp. Need to return just
    // the row we want to split on as midkey.
    Optional<ExtendedCell> optionalMidKey = reader.midKey();
    if (!optionalMidKey.isPresent()) {
      return Optional.empty();
    }
    Cell midKey = optionalMidKey.get();
    Cell firstKey = reader.getFirstKey().get();
    Cell lastKey = reader.getLastKey().get();
    // if the midkey is the same as the first or last keys, we cannot (ever) split this region.
    if (
      comparator.compareRows(midKey, firstKey) == 0 || comparator.compareRows(midKey, lastKey) == 0
    ) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("cannot split {} because midkey is the same as first or last row", file);
      }
      return Optional.empty();
    }
    return Optional.of(CellUtil.cloneRow(midKey));
  }

  /**
   * Gets the mid point of the largest file passed in as split point.
   */
  static Optional<byte[]> getSplitPoint(Collection<HStoreFile> storefiles,
    CellComparator comparator) throws IOException {
    Optional<HStoreFile> largestFile = StoreUtils.getLargestFile(storefiles);
    return largestFile.isPresent()
      ? StoreUtils.getFileSplitPoint(largestFile.get(), comparator)
      : Optional.empty();
  }

  /**
   * Returns the configured checksum algorithm.
   * @param conf The configuration
   * @return The checksum algorithm that is set in the configuration
   */
  public static ChecksumType getChecksumType(Configuration conf) {
    return ChecksumType.nameToType(
      conf.get(HConstants.CHECKSUM_TYPE_NAME, ChecksumType.getDefaultChecksumType().getName()));
  }

  /**
   * Returns the configured bytesPerChecksum value.
   * @param conf The configuration
   * @return The bytesPerChecksum that is set in the configuration
   */
  public static int getBytesPerChecksum(Configuration conf) {
    return conf.getInt(HConstants.BYTES_PER_CHECKSUM, HFile.DEFAULT_BYTES_PER_CHECKSUM);
  }

  /**
   * Build a store-specific configuration. Multi-tenant settings are table/cluster scoped and are
   * not expected at the column-family level, so we strip them from CF maps to preserve precedence.
   */
  public static Configuration createStoreConfiguration(Configuration conf, TableDescriptor td,
    ColumnFamilyDescriptor cfd) {
    // CompoundConfiguration will look for keys in reverse order of addition, so we'd
    // add global config first, then table and cf overrides, then cf metadata.
    Map<String, String> filteredCfConfiguration = stripMultiTenantKeys(cfd.getConfiguration());
    Map<Bytes, Bytes> filteredCfValues = stripMultiTenantKeysFromBytesMap(cfd.getValues());
    return new CompoundConfiguration().add(conf).addBytesMap(td.getValues())
      .addStringMap(filteredCfConfiguration).addBytesMap(filteredCfValues);
  }

  private static Map<String, String> stripMultiTenantKeys(Map<String, String> source) {
    if (source == null || source.isEmpty()) {
      return source;
    }
    Map<String, String> filtered = new HashMap<>(source);
    for (String key : MULTI_TENANT_CONF_KEYS) {
      filtered.remove(key);
    }
    return filtered;
  }

  private static Map<Bytes, Bytes> stripMultiTenantKeysFromBytesMap(Map<Bytes, Bytes> source) {
    if (source == null || source.isEmpty()) {
      return source;
    }
    Map<Bytes, Bytes> filtered = new HashMap<>();
    for (Map.Entry<Bytes, Bytes> entry : source.entrySet()) {
      Bytes key = entry.getKey();
      if (key == null || key.get() == null) {
        continue;
      }
      String keyString = Bytes.toString(key.get());
      if (!MULTI_TENANT_CONF_KEYS.contains(keyString)) {
        filtered.put(key, entry.getValue());
      }
    }
    return filtered;
  }

  public static List<StoreFileInfo> toStoreFileInfo(Collection<HStoreFile> storefiles) {
    return storefiles.stream().map(HStoreFile::getFileInfo).collect(Collectors.toList());
  }

  public static long getTotalUncompressedBytes(List<HStoreFile> files) {
    return files.stream()
      .mapToLong(file -> getStorefileFieldSize(file, StoreFileReader::getTotalUncompressedBytes))
      .sum();
  }

  public static long getStorefilesSize(Collection<HStoreFile> files,
    Predicate<HStoreFile> predicate) {
    return files.stream().filter(predicate)
      .mapToLong(file -> getStorefileFieldSize(file, StoreFileReader::length)).sum();
  }

  public static long getStorefileFieldSize(HStoreFile file, ToLongFunction<StoreFileReader> f) {
    if (file == null) {
      return 0L;
    }
    StoreFileReader reader = file.getReader();
    if (reader == null) {
      return 0L;
    }
    return f.applyAsLong(reader);
  }
}
