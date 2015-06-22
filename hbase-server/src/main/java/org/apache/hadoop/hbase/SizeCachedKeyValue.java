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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is an extension to KeyValue where rowLen and keyLen are cached.
 * Parsing the backing byte[] every time to get these values will affect the performance.
 * In read path, we tend to read these values many times in Comparator, SQM etc.
 * Note: Please do not use these objects in write path as it will increase the heap space usage.
 * See https://issues.apache.org/jira/browse/HBASE-13448
 */
@InterfaceAudience.Private
public class SizeCachedKeyValue extends KeyValue {

  private static final int HEAP_SIZE_OVERHEAD = Bytes.SIZEOF_SHORT + Bytes.SIZEOF_INT;

  private short rowLen;
  private int keyLen;

  public SizeCachedKeyValue(byte[] bytes, int offset, int length) {
    super(bytes, offset, length);
    // We will read all these cached values at least once. Initialize now itself so that we can
    // avoid uninitialized checks with every time call
    rowLen = super.getRowLength();
    keyLen = super.getKeyLength();
  }

  @Override
  public short getRowLength() {
    return rowLen;
  }

  @Override
  public int getKeyLength() {
    return this.keyLen;
  }

  @Override
  public long heapSize() {
    return super.heapSize() + HEAP_SIZE_OVERHEAD;
  }

  @Override
  public long heapSizeWithoutTags() {
    return super.heapSizeWithoutTags() + HEAP_SIZE_OVERHEAD;
  }
}