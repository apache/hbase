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
import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface BlockIndexChunk {

  List<byte[]> getBlockKeys();

  List<Integer> getSecondaryIndexOffsetMarks();

  int getEntryBySubEntry(long k);

  void add(byte[] firstKey, long blockOffset, int onDiskDataSize);

  void add(byte[] firstKey, long blockOffset, int onDiskDataSize, long curTotalNumSubEntries);

  int getRootSize();

  int getCurTotalNonRootEntrySize();

  int getNonRootSize();

  int getNumEntries();

  byte[] getBlockKey(int i);

  long getBlockOffset(int i);

  int getOnDiskDataSize(int i);

  byte[] getMidKeyMetadata() throws IOException;

  void clear();
}
