/**
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
package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.yetus.audience.InterfaceAudience;

import java.util.Collection;

/**
 * Coprocessors use this interface to get details about compaction.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
public interface CompactionRequest {

  /**
   * @return unmodifiable collection of StoreFiles in compaction
   */
  Collection<? extends StoreFile> getFiles();

  /**
   * @return total size of all StoreFiles in compaction
   */
  long getSize();

  /**
   * @return <code>true</code> if major compaction or all files are compacted
   */
  boolean isAllFiles();

  /**
   * @return <code>true</code> if major compaction
   */
  boolean isMajor();

  /**
   * @return priority of compaction request
   */
  int getPriority();

  /**
   * @return <code>true</code> if compaction is Off-peak
   */
  boolean isOffPeak();

  /**
   * @return compaction request creation time in milliseconds
   */
  long getSelectionTime();

}
