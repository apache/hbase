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

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating TenantExtractor instances based on the HFile context.
 */
@InterfaceAudience.Private
public class TenantExtractorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TenantExtractorFactory.class);
  
  /**
   * Create a tenant extractor based on HFile context.
   * @param context HFile context containing tenant configuration
   * @return A configured TenantExtractor or null if multi-tenant features are not enabled
   */
  public static TenantExtractor createTenantExtractor(HFileContext context) {
    if (context == null) {
      return null;
    }
    
    // Check if the multi-tenant feature is enabled
    if (!context.isMultiTenant()) {
      return null;
    }
    
    // Get prefix configuration from the context
    int prefixLength = context.getPbePrefixLength();
    int prefixOffset = context.getPrefixOffset();
    
    // Create and return a DefaultTenantExtractor with the configured parameters
    if (prefixLength > 0) {
      DefaultTenantExtractor extractor = new DefaultTenantExtractor(prefixLength, prefixOffset);
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Created DefaultTenantExtractor with prefixLength={}, prefixOffset={}", 
            prefixLength, prefixOffset);
      }
      
      return extractor;
    }
    
    // If prefix length is not positive, multi-tenant features won't work properly
    LOG.warn("Multi-tenant HFile feature enabled but invalid prefix length: {}", prefixLength);
    return null;
  }
} 