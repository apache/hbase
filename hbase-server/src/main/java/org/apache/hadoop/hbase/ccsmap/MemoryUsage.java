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
public final class MemoryUsage {
  long totalSize;

  long dataSpace;
  long trashDataSpace;
  long maxDataSpae;

  long heapKVCapacity;
  long maxHeapKVCapacity;
  long trashHeapKVCount;
  long trashHeapKVSize;
  long aliveHeapKVCount;
  long aliveHeapKVSize;

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CompactedConcurrentSkipListMapMemoryUsage:[TotalSize:")
      .append(aliveHeapKVSize + trashHeapKVSize + dataSpace);
    sb.append(", DataSpace:").append(dataSpace);
    sb.append(", TrashData:").append(trashDataSpace);
    sb.append(", HeapKVCapacity:").append(heapKVCapacity);
    sb.append(", MaxHeapKVCapacity:").append(maxHeapKVCapacity);
    sb.append(", AliveHeapKVSize:").append(aliveHeapKVSize);
    sb.append(", AliveHeapKVCount:").append(aliveHeapKVCount);
    sb.append(", TrashHeapKVSize:").append(trashHeapKVSize);
    sb.append(", TrashHeapKVCount:").append(trashHeapKVCount);
    sb.append("]");
    return sb.toString();
  }

}
