/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.ccsmap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public class PageSetting {
  // Single data page's size, must be power of 2
  int pageSize = CCSMapUtils.DEFAULT_PAGE_SIZE;
  // The max data page this map can support;
  int pages = CCSMapUtils.DEFAULT_PAGES;
  // Single heapKV page's size, must be power of 2
  int heapKVPageSize = CCSMapUtils.DEFAULT_HEAPKV_PAGE_SIZE;
  // The max heapKV page this map can support
  int heapKVPages = CCSMapUtils.DEFAULT_HEAPKV_PAGES;
  // KV's size beyond heapKVThreshold will be placed on heap instead of on
  int heapKVThreshold = CCSMapUtils.DEFAULT_PAGE_SIZE;

  PageAllocator pageAllocator = null;

  public static PageSetting create() {
    return new PageSetting();
  }

  public PageSetting setDataPageSize(int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  public PageSetting setPages(int maxPages) {
    this.pages = maxPages;
    return this;
  }

  public PageSetting setHeapKVPageSize(int heapKVPageSize) {
    this.heapKVPageSize = heapKVPageSize;
    return this;
  }

  public PageSetting setHeapKVPages(int heapKVPages) {
    this.heapKVPages = heapKVPages;
    return this;
  }

  public PageSetting setHeapKVThreshold(int heapKVThreshold) {
    this.heapKVThreshold = heapKVThreshold;
    return this;
  }

  public PageSetting setPageAllocator(PageAllocator allocator) {
    this.pageAllocator = allocator;
    return this;
  }

}
