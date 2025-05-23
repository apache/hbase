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
 * Tenant configuration is obtained from cluster configuration and table properties,
 * not from HFileContext.
 */
@InterfaceAudience.Private
public class TenantExtractorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TenantExtractorFactory.class);
  
  // Default values
  private static final int DEFAULT_PREFIX_LENGTH = 4;
  
  /**
   * Create a tenant extractor based on configuration.
   * This applies configuration with proper precedence:
   * 1. Table level settings have highest precedence
   * 2. Cluster level settings are used as fallback
   * 3. Default values are used if neither is specified
   * 
   * @param conf HBase configuration for cluster defaults
   * @param tableProperties Table properties for table-specific settings
   * @return A configured TenantExtractor
   */
  public static TenantExtractor createTenantExtractor(
      Configuration conf, Map<String, String> tableProperties) {
    
    // Check if multi-tenant functionality is enabled for this table
    boolean multiTenantEnabled = false; // Default to disabled - only enabled when explicitly set
    if (tableProperties != null && tableProperties.containsKey(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED)) {
      multiTenantEnabled = Boolean.parseBoolean(tableProperties.get(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED));
    }
    
    // If multi-tenant is disabled, return SingleTenantExtractor
    if (!multiTenantEnabled) {
      LOG.info("Multi-tenant functionality disabled for this table, using SingleTenantExtractor");
      return new MultiTenantHFileWriter.SingleTenantExtractor();
    }
    
    // Multi-tenant enabled - configure DefaultTenantExtractor
    
    // First try table level settings (highest precedence)
    String tablePrefixLengthStr = tableProperties != null ? 
        tableProperties.get(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH) : null;
    
    // If not found at table level, try cluster level settings
    int clusterPrefixLength = conf.getInt(
        MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, DEFAULT_PREFIX_LENGTH);
    
    // Use table settings if available, otherwise use cluster settings
    int prefixLength;
    if (tablePrefixLengthStr != null) {
      try {
        prefixLength = Integer.parseInt(tablePrefixLengthStr);
      } catch (NumberFormatException nfe) {
        LOG.warn("Invalid table-level tenant prefix length '{}', using cluster default {}", tablePrefixLengthStr, clusterPrefixLength);
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