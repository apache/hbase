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
package org.apache.hadoop.hbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.ExploringCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.FIFOCompactionPolicy;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

/**
 * Only used for master to sanity check {@link org.apache.hadoop.hbase.client.TableDescriptor}.
 */
@InterfaceAudience.Private
public final class TableDescriptorChecker {
  private static Logger LOG = LoggerFactory.getLogger(TableDescriptorChecker.class);

  public static final String TABLE_SANITY_CHECKS = "hbase.table.sanity.checks";
  public static final boolean DEFAULT_TABLE_SANITY_CHECKS = true;

  //should we check the compression codec type at master side, default true, HBASE-6370
  public static final String MASTER_CHECK_COMPRESSION = "hbase.master.check.compression";
  public static final boolean DEFAULT_MASTER_CHECK_COMPRESSION = true;

  //should we check encryption settings at master side, default true
  public static final String MASTER_CHECK_ENCRYPTION = "hbase.master.check.encryption";
  public static final boolean DEFAULT_MASTER_CHECK_ENCRYPTION = true;

  private TableDescriptorChecker() {
  }

  /**
   * Checks whether the table conforms to some sane limits, and configured
   * values (compression, etc) work. Throws an exception if something is wrong.
   */
  public static void sanityCheck(final Configuration c, final TableDescriptor td)
      throws IOException {
    CompoundConfiguration conf = new CompoundConfiguration()
      .add(c)
      .addBytesMap(td.getValues());

    // Setting this to true logs the warning instead of throwing exception
    boolean logWarn = false;
    if (!conf.getBoolean(TABLE_SANITY_CHECKS, DEFAULT_TABLE_SANITY_CHECKS)) {
      logWarn = true;
    }
    String tableVal = td.getValue(TABLE_SANITY_CHECKS);
    if (tableVal != null && !Boolean.valueOf(tableVal)) {
      logWarn = true;
    }

    // check max file size
    long maxFileSizeLowerLimit = 2 * 1024 * 1024L; // 2M is the default lower limit
    // if not set MAX_FILESIZE in TableDescriptor, and not set HREGION_MAX_FILESIZE in
    // hbase-site.xml, use maxFileSizeLowerLimit instead to skip this check
    long maxFileSize = td.getValue(TableDescriptorBuilder.MAX_FILESIZE) == null ?
      conf.getLong(HConstants.HREGION_MAX_FILESIZE, maxFileSizeLowerLimit) :
      Long.parseLong(td.getValue(TableDescriptorBuilder.MAX_FILESIZE));
    if (maxFileSize < conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit)) {
      String message =
          "MAX_FILESIZE for table descriptor or " + "\"hbase.hregion.max.filesize\" (" +
              maxFileSize + ") is too small, which might cause over splitting into unmanageable " +
              "number of regions.";
      warnOrThrowExceptionForFailure(logWarn, message, null);
    }

    // check flush size
    long flushSizeLowerLimit = 1024 * 1024L; // 1M is the default lower limit
    // if not set MEMSTORE_FLUSHSIZE in TableDescriptor, and not set HREGION_MEMSTORE_FLUSH_SIZE in
    // hbase-site.xml, use flushSizeLowerLimit instead to skip this check
    long flushSize = td.getValue(TableDescriptorBuilder.MEMSTORE_FLUSHSIZE) == null ?
      conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, flushSizeLowerLimit) :
      Long.parseLong(td.getValue(TableDescriptorBuilder.MEMSTORE_FLUSHSIZE));
    if (flushSize < conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit)) {
      String message = "MEMSTORE_FLUSHSIZE for table descriptor or " +
          "\"hbase.hregion.memstore.flush.size\" (" + flushSize +
          ") is too small, which might cause" + " very frequent flushing.";
      warnOrThrowExceptionForFailure(logWarn, message, null);
    }

    // check that coprocessors and other specified plugin classes can be loaded
    try {
      checkClassLoading(conf, td);
    } catch (Exception ex) {
      warnOrThrowExceptionForFailure(logWarn, ex.getMessage(), null);
    }

    if (conf.getBoolean(MASTER_CHECK_COMPRESSION, DEFAULT_MASTER_CHECK_COMPRESSION)) {
      // check compression can be loaded
      try {
        checkCompression(td);
      } catch (IOException e) {
        warnOrThrowExceptionForFailure(logWarn, e.getMessage(), e);
      }
    }

    if (conf.getBoolean(MASTER_CHECK_ENCRYPTION, DEFAULT_MASTER_CHECK_ENCRYPTION)) {
      // check encryption can be loaded
      try {
        checkEncryption(conf, td);
      } catch (IOException e) {
        warnOrThrowExceptionForFailure(logWarn, e.getMessage(), e);
      }
    }

    // Verify compaction policy
    try {
      checkCompactionPolicy(conf, td);
    } catch (IOException e) {
      warnOrThrowExceptionForFailure(false, e.getMessage(), e);
    }
    // check that we have at least 1 CF
    if (td.getColumnFamilyCount() == 0) {
      String message = "Table should have at least one column family.";
      warnOrThrowExceptionForFailure(logWarn, message, null);
    }

    // check that we have minimum 1 region replicas
    int regionReplicas = td.getRegionReplication();
    if (regionReplicas < 1) {
      String message = "Table region replication should be at least one.";
      warnOrThrowExceptionForFailure(logWarn, message, null);
    }

    // Meta table shouldn't be set as read only, otherwise it will impact region assignments
    if (td.isReadOnly() && TableName.isMetaTableName(td.getTableName())) {
      warnOrThrowExceptionForFailure(false, "Meta table can't be set as read only.", null);
    }

    for (ColumnFamilyDescriptor hcd : td.getColumnFamilies()) {
      if (hcd.getTimeToLive() <= 0) {
        String message = "TTL for column family " + hcd.getNameAsString() + " must be positive.";
        warnOrThrowExceptionForFailure(logWarn, message, null);
      }

      // check blockSize
      if (hcd.getBlocksize() < 1024 || hcd.getBlocksize() > 16 * 1024 * 1024) {
        String message = "Block size for column family " + hcd.getNameAsString() +
            "  must be between 1K and 16MB.";
        warnOrThrowExceptionForFailure(logWarn, message, null);
      }

      // check versions
      if (hcd.getMinVersions() < 0) {
        String message =
            "Min versions for column family " + hcd.getNameAsString() + "  must be positive.";
        warnOrThrowExceptionForFailure(logWarn, message, null);
      }
      // max versions already being checked

      // HBASE-13776 Setting illegal versions for ColumnFamilyDescriptor
      //  does not throw IllegalArgumentException
      // check minVersions <= maxVerions
      if (hcd.getMinVersions() > hcd.getMaxVersions()) {
        String message = "Min versions for column family " + hcd.getNameAsString() +
            " must be less than the Max versions.";
        warnOrThrowExceptionForFailure(logWarn, message, null);
      }

      // check replication scope
      checkReplicationScope(hcd);
      // check bloom filter type
      checkBloomFilterType(hcd);

      // check data replication factor, it can be 0(default value) when user has not explicitly
      // set the value, in this case we use default replication factor set in the file system.
      if (hcd.getDFSReplication() < 0) {
        String message = "HFile Replication for column family " + hcd.getNameAsString() +
            "  must be greater than zero.";
        warnOrThrowExceptionForFailure(logWarn, message, null);
      }

      // check in-memory compaction
      try {
        hcd.getInMemoryCompaction();
      } catch (IllegalArgumentException e) {
        warnOrThrowExceptionForFailure(logWarn, e.getMessage(), e);
      }
    }
  }

  private static void checkReplicationScope(final ColumnFamilyDescriptor cfd) throws IOException {
    // check replication scope
    WALProtos.ScopeType scop = WALProtos.ScopeType.valueOf(cfd.getScope());
    if (scop == null) {
      String message =
          "Replication scope for column family " + cfd.getNameAsString() + " is " + cfd.getScope() +
              " which is invalid.";

      LOG.error(message);
      throw new DoNotRetryIOException(message);
    }
  }

  private static void checkCompactionPolicy(Configuration conf, TableDescriptor td)
      throws IOException {
    // FIFO compaction has some requirements
    // Actually FCP ignores periodic major compactions
    String className = td.getValue(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY);
    if (className == null) {
      className = conf.get(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY,
          ExploringCompactionPolicy.class.getName());
    }

    int blockingFileCount = HStore.DEFAULT_BLOCKING_STOREFILE_COUNT;
    String sv = td.getValue(HStore.BLOCKING_STOREFILES_KEY);
    if (sv != null) {
      blockingFileCount = Integer.parseInt(sv);
    } else {
      blockingFileCount = conf.getInt(HStore.BLOCKING_STOREFILES_KEY, blockingFileCount);
    }

    for (ColumnFamilyDescriptor hcd : td.getColumnFamilies()) {
      String compactionPolicy =
          hcd.getConfigurationValue(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY);
      if (compactionPolicy == null) {
        compactionPolicy = className;
      }
      if (!compactionPolicy.equals(FIFOCompactionPolicy.class.getName())) {
        continue;
      }
      // FIFOCompaction
      String message = null;

      // 1. Check TTL
      if (hcd.getTimeToLive() == ColumnFamilyDescriptorBuilder.DEFAULT_TTL) {
        message = "Default TTL is not supported for FIFO compaction";
        throw new IOException(message);
      }

      // 2. Check min versions
      if (hcd.getMinVersions() > 0) {
        message = "MIN_VERSION > 0 is not supported for FIFO compaction";
        throw new IOException(message);
      }

      // 3. blocking file count
      sv = hcd.getConfigurationValue(HStore.BLOCKING_STOREFILES_KEY);
      if (sv != null) {
        blockingFileCount = Integer.parseInt(sv);
      }
      if (blockingFileCount < 1000) {
        message =
            "Blocking file count '" + HStore.BLOCKING_STOREFILES_KEY + "' " + blockingFileCount +
                " is below recommended minimum of 1000 for column family " + hcd.getNameAsString();
        throw new IOException(message);
      }
    }
  }

  private static void checkBloomFilterType(ColumnFamilyDescriptor cfd) throws IOException {
    Configuration conf = new CompoundConfiguration().addStringMap(cfd.getConfiguration());
    try {
      BloomFilterUtil.getBloomFilterParam(cfd.getBloomFilterType(), conf);
    } catch (IllegalArgumentException e) {
      throw new DoNotRetryIOException("Failed to get bloom filter param", e);
    }
  }

  public static void checkCompression(final TableDescriptor td) throws IOException {
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      CompressionTest.testCompression(cfd.getCompressionType());
      CompressionTest.testCompression(cfd.getCompactionCompressionType());
    }
  }

  public static void checkEncryption(final Configuration conf, final TableDescriptor td)
      throws IOException {
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      EncryptionTest.testEncryption(conf, cfd.getEncryptionType(), cfd.getEncryptionKey());
    }
  }

  public static void checkClassLoading(final Configuration conf, final TableDescriptor td)
      throws IOException {
    RegionSplitPolicy.getSplitPolicyClass(td, conf);
    RegionCoprocessorHost.testTableCoprocessorAttrs(conf, td);
  }

  // HBASE-13350 - Helper method to log warning on sanity check failures if checks disabled.
  private static void warnOrThrowExceptionForFailure(boolean logWarn, String message,
      Exception cause) throws IOException {
    if (!logWarn) {
      throw new DoNotRetryIOException(message + " Set " + TABLE_SANITY_CHECKS +
          " to false at conf or table descriptor if you want to bypass sanity checks", cause);
    }
    LOG.warn(message);
  }
}
