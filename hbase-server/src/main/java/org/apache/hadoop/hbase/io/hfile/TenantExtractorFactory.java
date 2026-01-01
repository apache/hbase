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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating TenantExtractor instances based on configuration.
 * <p>
 * Tenant configuration is obtained from cluster configuration and table properties, not from
 * HFileContext.
 * <p>
 * For HFile v4, tenant configuration is stored in the file trailer, allowing it to be accessed
 * before the file info blocks are loaded. This resolves timing issues in the reader initialization
 * process.
 */
@InterfaceAudience.Private
public class TenantExtractorFactory {
  /** Logger for this class */
  private static final Logger LOG = LoggerFactory.getLogger(TenantExtractorFactory.class);

  private TenantExtractorFactory() {
    // Utility class, no instantiation
  }

  /** Default tenant prefix length when not specified in configuration */
  private static final int DEFAULT_PREFIX_LENGTH = 4;

  /**
   * Create a TenantExtractor from HFile's reader context. This method is called during HFile
   * reading to determine how to extract tenant information.
   * @param reader The HFile reader that contains file info
   * @return Appropriate TenantExtractor implementation
   */
  public static TenantExtractor createFromReader(HFile.Reader reader) {
    // Check if this is a v4 file with tenant configuration in the trailer
    FixedFileTrailer trailer = reader.getTrailer();
    if (trailer.getMajorVersion() == HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT) {
      if (trailer.isMultiTenant()) {
        int prefixLength = trailer.getTenantPrefixLength();
        LOG.info("Multi-tenant enabled from HFile v4 trailer, prefixLength={}", prefixLength);
        return new DefaultTenantExtractor(prefixLength);
      } else {
        LOG.info("HFile v4 format, but multi-tenant not enabled in trailer");
        return new MultiTenantHFileWriter.SingleTenantExtractor();
      }
    }

    // For non-v4 files, always use SingleTenantExtractor
    LOG.info("Non-v4 HFile format (v{}), using SingleTenantExtractor", trailer.getMajorVersion());
    return new MultiTenantHFileWriter.SingleTenantExtractor();
  }

  /**
   * Create a tenant extractor based on configuration. This applies configuration with proper
   * precedence: 1. Table level settings have highest precedence 2. Cluster level settings are used
   * as fallback 3. Default values are used if neither is specified
   * @param conf            HBase configuration for cluster defaults
   * @param tableProperties Table properties for table-specific settings
   * @return A configured TenantExtractor
   */
  public static TenantExtractor createTenantExtractor(Configuration conf,
    Map<String, String> tableProperties) {

    // Check if multi-tenant functionality is enabled for this table
    //
    // IMPORTANT:
    // - In regionserver/master write paths, the `conf` passed here is usually a Store-specific
    //   CompoundConfiguration that already includes table descriptor values (see
    //   StoreUtils#createStoreConfiguration).
    // - In other call paths, callers may provide `tableProperties` explicitly.
    //
    // We therefore support both, with precedence: tableProperties > conf > default(false).
    String multiTenantEnabledStr = tableProperties != null
      ? tableProperties.get(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED)
      : null;
    if (multiTenantEnabledStr == null) {
      multiTenantEnabledStr = conf.get(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED);
    }
    boolean multiTenantEnabled = Boolean.parseBoolean(multiTenantEnabledStr);

    // If multi-tenant is disabled, return SingleTenantExtractor
    if (!multiTenantEnabled) {
      LOG.info("Multi-tenant functionality disabled for this table, using SingleTenantExtractor");
      return new MultiTenantHFileWriter.SingleTenantExtractor();
    }

    // Multi-tenant enabled - configure DefaultTenantExtractor

    // First try table level settings (highest precedence)
    String tablePrefixLengthStr = tableProperties != null
      ? tableProperties.get(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH)
      : null;
    if (tablePrefixLengthStr == null) {
      tablePrefixLengthStr = conf.get(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH);
    }

    // If not found at table level, try cluster level settings
    int clusterPrefixLength =
      conf.getInt(MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, DEFAULT_PREFIX_LENGTH);

    // Use table settings if available, otherwise use cluster settings
    int prefixLength;
    if (tablePrefixLengthStr != null) {
      try {
        prefixLength = Integer.parseInt(tablePrefixLengthStr);
      } catch (NumberFormatException nfe) {
        LOG.warn("Invalid table-level tenant prefix length '{}', using cluster default {}",
          tablePrefixLengthStr, clusterPrefixLength);
        prefixLength = clusterPrefixLength;
      }
    } else {
      prefixLength = clusterPrefixLength;
    }

    LOG.info("Tenant configuration initialized: prefixLength={}, from table properties: {}",
      prefixLength, (tablePrefixLengthStr != null));

    // Create and return a DefaultTenantExtractor with the configured parameters
    return new DefaultTenantExtractor(prefixLength);
  }
}
