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

import com.google.errorprone.annotations.RestrictedApi;
import java.util.List;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

@InterfaceAudience.Private
final class StoreFilePathUpdate {

  private final List<Path> storeFiles;

  private StoreFilePathUpdate(final List<Path> storeFiles) {
    Preconditions.checkNotNull(storeFiles, "StoreFiles cannot be null");
    this.storeFiles = storeFiles;
  }

  List<Path> getStoreFiles() {
    return storeFiles;
  }

  @Override
  public String toString() {
    return "StoreFilePathUpdate{" + "storeFiles=" + storeFiles + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StoreFilePathUpdate that = (StoreFilePathUpdate) o;

    return new EqualsBuilder().append(storeFiles, that.storeFiles).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(storeFiles).toHashCode();
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private List<Path> storeFiles = ImmutableList.of();

    Builder withStoreFiles(List<HStoreFile> storeFiles) {
      this.storeFiles = StoreFileTrackingUtils.convertStoreFilesToPaths(storeFiles);
      return this;
    }

    @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
    Builder withStorePaths(List<Path> storeFiles) {
      this.storeFiles = storeFiles;
      return this;
    }

    StoreFilePathUpdate build() {
      return new StoreFilePathUpdate(storeFiles);
    }
  }
}
