/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.BloomFilterFactory;

/**
 *
 * Container class which holds several MemstoreBloomFilters and applies the
 * add, snapshot and clearSnapshot functions on all the members.
 *
 * Adding a new BloomFilter to this container will require us to
 * add the corresponding builder method and respective contains method.
 */
public class MemstoreBloomFilterContainer {
  private final MemstoreRowPrefixBloomFilter rowPrefixBloomFilter;
  private final boolean enabled;
  private static final Log LOG =
    LogFactory.getLog(MemstoreBloomFilterContainer.class);
  private MemstoreBloomFilterContainer(
      MemstoreRowPrefixBloomFilter rowPrefixBloomFilter,
      Configuration conf) {
    this.rowPrefixBloomFilter = rowPrefixBloomFilter;
    this.enabled = conf.getBoolean(HConstants.IN_MEMORY_BLOOM_ENABLED,
        HConstants.DEFAULT_IN_MEMORY_BLOOM_ENABLED);
  }

  public void add(KeyValue kv) {
    if (!enabled) return;
    if (rowPrefixBloomFilter != null) rowPrefixBloomFilter.add(kv);
  }

  public boolean containsRowPrefixForKeyValue(KeyValue kv) {
    if (!enabled) return true;
    if (rowPrefixBloomFilter != null) {
      return rowPrefixBloomFilter.contains(kv);
    }
    return true;
  }

  public static class Builder {
    private MemstoreRowPrefixBloomFilter rowPrefixBloomFilter = null;
    private Configuration conf;
    public Builder(Configuration conf) {
      this.conf = conf;
    }

    /**
     * Adds a MemstoreRowPrefixBloomFilter to the MemstoreBloomFilterContainer.
     * @param rowPrefix
     * @return
     */
    public Builder withRowPrefixFilter(int rowPrefix) {
      MemstoreRowPrefixBloomFilter rpbf = null;
      if (conf.getBoolean(
          BloomFilterFactory.IO_STOREFILE_ROWKEYPREFIX_BLOOM_ENABLED, false) &&
          rowPrefix > 0) {
        rpbf = new MemstoreRowPrefixBloomFilter(rowPrefix, this.conf);
      }
      if (rpbf == null) {
        LOG.info("Row Key Prefix bloom filter disabled in table schema");
      }
      rowPrefixBloomFilter = rpbf;
      return this;
    }

    /**
     * Creates the actual MemstoreBloomFilterContainer
     * @return
     */
    public MemstoreBloomFilterContainer create() {
      return new MemstoreBloomFilterContainer(
          this.rowPrefixBloomFilter,
          this.conf);
    }
  }
}
