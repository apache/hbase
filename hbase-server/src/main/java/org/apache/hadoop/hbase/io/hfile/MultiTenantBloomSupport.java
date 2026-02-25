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

import java.io.IOException;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Internal contract that enables multi-tenant HFile readers to participate in Bloom filter
 * decisions while staying transparent to existing StoreFileReader callers.
 */
@InterfaceAudience.Private
public interface MultiTenantBloomSupport {

  boolean passesGeneralRowBloomFilter(byte[] row, int rowOffset, int rowLen) throws IOException;

  boolean passesGeneralRowColBloomFilter(ExtendedCell cell) throws IOException;

  boolean passesDeleteFamilyBloomFilter(byte[] row, int rowOffset, int rowLen) throws IOException;

  BloomType getGeneralBloomFilterType();

  int getGeneralBloomPrefixLength() throws IOException;

  byte[] getLastBloomKey() throws IOException;

  long getDeleteFamilyBloomCount() throws IOException;

  BloomFilter getGeneralBloomFilterInstance() throws IOException;

  BloomFilter getDeleteFamilyBloomFilterInstance() throws IOException;
}
