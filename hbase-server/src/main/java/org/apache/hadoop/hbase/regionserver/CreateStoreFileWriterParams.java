/**
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

import java.util.function.Consumer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class CreateStoreFileWriterParams {

  private long maxKeyCount;

  private Compression.Algorithm compression;

  private boolean isCompaction;

  private boolean includeMVCCReadpoint;

  private boolean includesTag;

  private boolean shouldDropBehind;

  private long totalCompactedFilesSize = -1;

  private String fileStoragePolicy = HConstants.EMPTY_STRING;

  private Consumer<Path> writerCreationTracker;

  private CreateStoreFileWriterParams() {
  }

  public long maxKeyCount() {
    return maxKeyCount;
  }

  public CreateStoreFileWriterParams maxKeyCount(long maxKeyCount) {
    this.maxKeyCount = maxKeyCount;
    return this;
  }

  public Compression.Algorithm compression() {
    return compression;
  }

  /**
   * Set the compression algorithm to use
   */
  public CreateStoreFileWriterParams compression(Compression.Algorithm compression) {
    this.compression = compression;
    return this;
  }

  public boolean isCompaction() {
    return isCompaction;
  }

  /**
   * Whether we are creating a new file in a compaction
   */
  public CreateStoreFileWriterParams isCompaction(boolean isCompaction) {
    this.isCompaction = isCompaction;
    return this;
  }

  public boolean includeMVCCReadpoint() {
    return includeMVCCReadpoint;
  }

  /**
   * Whether to include MVCC or not
   */
  public CreateStoreFileWriterParams includeMVCCReadpoint(boolean includeMVCCReadpoint) {
    this.includeMVCCReadpoint = includeMVCCReadpoint;
    return this;
  }

  public boolean includesTag() {
    return includesTag;
  }

  /**
   * Whether to includesTag or not
   */
  public CreateStoreFileWriterParams includesTag(boolean includesTag) {
    this.includesTag = includesTag;
    return this;
  }

  public boolean shouldDropBehind() {
    return shouldDropBehind;
  }

  public CreateStoreFileWriterParams shouldDropBehind(boolean shouldDropBehind) {
    this.shouldDropBehind = shouldDropBehind;
    return this;
  }

  public long totalCompactedFilesSize() {
    return totalCompactedFilesSize;
  }

  public CreateStoreFileWriterParams totalCompactedFilesSize(long totalCompactedFilesSize) {
    this.totalCompactedFilesSize = totalCompactedFilesSize;
    return this;
  }

  public String fileStoragePolicy() {
    return fileStoragePolicy;
  }

  public CreateStoreFileWriterParams fileStoragePolicy(String fileStoragePolicy) {
    this.fileStoragePolicy = fileStoragePolicy;
    return this;
  }

  public Consumer<Path> writerCreationTracker() {
    return writerCreationTracker;
  }

  public CreateStoreFileWriterParams writerCreationTracker(Consumer<Path> writerCreationTracker) {
    this.writerCreationTracker = writerCreationTracker;
    return this;
  }

  public static CreateStoreFileWriterParams create() {
    return new CreateStoreFileWriterParams();
  }
}
