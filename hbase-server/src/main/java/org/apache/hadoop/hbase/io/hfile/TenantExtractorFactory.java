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
  private static final int DEFAULT_PREFIX_OFFSET = 0;
  
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
    
    // First try table level settings (highest precedence)
    String tablePrefixLengthStr = tableProperties != null ? 
        tableProperties.get(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH) : null;
    String tablePrefixOffsetStr = tableProperties != null ? 
        tableProperties.get(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_OFFSET) : null;
    
    // If not found at table level, try cluster level settings
    int clusterPrefixLength = conf.getInt(
        MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, DEFAULT_PREFIX_LENGTH);
    int clusterPrefixOffset = conf.getInt(
        MultiTenantHFileWriter.TENANT_PREFIX_OFFSET, DEFAULT_PREFIX_OFFSET);
    
    // Use table settings if available, otherwise use cluster settings
    int prefixLength = tablePrefixLengthStr != null ? 
        Integer.parseInt(tablePrefixLengthStr) : clusterPrefixLength;
    int prefixOffset = tablePrefixOffsetStr != null ? 
        Integer.parseInt(tablePrefixOffsetStr) : clusterPrefixOffset;
    
    LOG.info("Tenant configuration initialized: prefixLength={}, prefixOffset={}, " +
        "from table properties: {}", prefixLength, prefixOffset, 
        (tablePrefixLengthStr != null || tablePrefixOffsetStr != null));
    
    // Create and return a DefaultTenantExtractor with the configured parameters
    return new DefaultTenantExtractor(prefixLength, prefixOffset);
  }
} 