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

package org.apache.hadoop.hbase.regionserver;

import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Params for UpdateReader - To be used for Scanner Notification during flush/compaction
 */
@InterfaceAudience.Private
final public class UpdateReaderParams {

  private List<HStoreFile> storeFiles;

  private List<KeyValueScanner> memStoreScanners;

  private boolean isFlushEvent;

  private UpdateReaderParams(List<HStoreFile> storeFiles,
      List<KeyValueScanner> memStoreScanners, boolean isFlushEvent) {
    this.storeFiles = storeFiles;
    this.memStoreScanners = memStoreScanners;
    this.isFlushEvent = isFlushEvent;
  }

  public List<HStoreFile> getStoreFiles() {
    return storeFiles;
  }

  public List<KeyValueScanner> getMemStoreScanners() {
    return memStoreScanners;
  }

  public boolean isFlushEvent() {
    return isFlushEvent;
  }

  public static class UpdateReaderParamsBuilder {
    private List<HStoreFile> storeFiles;
    private List<KeyValueScanner> memStoreScanners;
    private boolean isFlushEvent;

    public UpdateReaderParamsBuilder setStoreFiles(List<HStoreFile> storeFiles) {
      this.storeFiles = storeFiles;
      return this;
    }

    public UpdateReaderParamsBuilder setMemStoreScanners(
        List<KeyValueScanner> memStoreScanners) {
      this.memStoreScanners = memStoreScanners;
      return this;
    }

    public UpdateReaderParamsBuilder setIsFlushEvent(boolean isFlushEvent) {
      this.isFlushEvent = isFlushEvent;
      return this;
    }

    public UpdateReaderParams build() {
      return new UpdateReaderParams(storeFiles, memStoreScanners, isFlushEvent);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("UpdateReaderParams{");
    sb.append("storeFiles=").append(storeFiles);
    sb.append(", memStoreScanners=").append(memStoreScanners);
    sb.append(", isFlushEvent=").append(isFlushEvent);
    sb.append('}');
    return sb.toString();
  }

}
