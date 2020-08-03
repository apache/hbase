/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.List;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

@InterfaceAudience.Private
class StoreFilePathUpdate {

  private final List<Path> storeFiles;
  private final boolean hasStoreFilesUpdate;

  private StoreFilePathUpdate(final List<Path> storeFiles, boolean hasStoreFilesUpdate) {
    Preconditions.checkArgument(hasStoreFilesUpdate,
      "StoreFilePathUpdate must include an update");
    Preconditions.checkNotNull(storeFiles, "StoreFiles cannot be null");
    if (hasStoreFilesUpdate) {
      Preconditions.checkArgument(CollectionUtils.isNotEmpty(storeFiles), "StoreFilePaths cannot be empty");
    }
    this.storeFiles = storeFiles;
    this.hasStoreFilesUpdate = hasStoreFilesUpdate;
  }

  List<Path> getStoreFiles() {
    return storeFiles;
  }

  boolean hasStoreFilesUpdate() {
    return hasStoreFilesUpdate;
  }

  @Override
  public String toString() {
    return "StoreFilePathUpdate{" + "storeFiles=" + storeFiles + ", hasStoreFilesUpdate="
      + hasStoreFilesUpdate + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    StoreFilePathUpdate that = (StoreFilePathUpdate) o;

    return new EqualsBuilder().append(hasStoreFilesUpdate, that.hasStoreFilesUpdate)
      .append(storeFiles, that.storeFiles).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(storeFiles)
      .append(hasStoreFilesUpdate).toHashCode();
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private List<Path> storeFiles = ImmutableList.of();
    private boolean hasStoreFilesUpdate;

    Builder withStoreFiles(List<HStoreFile> storeFiles) {
      Preconditions.checkArgument(!hasStoreFilesUpdate,
        "Specify a Path List or File List, but not both");
      this.storeFiles = StorefileTrackingUtils.convertStoreFilesToPaths(storeFiles);
      this.hasStoreFilesUpdate = true;
      return this;
    }

    Builder withStorePaths(List<Path> storeFiles) {
      Preconditions.checkArgument(!hasStoreFilesUpdate,
        "Specify a Path List or File List, but not both");
      this.storeFiles = storeFiles;
      this.hasStoreFilesUpdate = true;
      return this;
    }

    StoreFilePathUpdate build() {
      return new StoreFilePathUpdate(storeFiles, hasStoreFilesUpdate);
    }
  }
}
