/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas.policies;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.SpaceLimitingException;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Abstract implementation for {@link SpaceViolationPolicyEnforcement}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AbstractViolationPolicyEnforcement
    implements SpaceViolationPolicyEnforcement {

  RegionServerServices rss;
  TableName tableName;
  SpaceQuotaSnapshot quotaSnapshot;

  public void setRegionServerServices(RegionServerServices rss) {
    this.rss = Objects.requireNonNull(rss);
  }

  public void setTableName(TableName tableName) {
    this.tableName = tableName;
  }

  public RegionServerServices getRegionServerServices() {
    return this.rss;
  }

  public TableName getTableName() {
    return this.tableName;
  }

  public void setQuotaSnapshot(SpaceQuotaSnapshot snapshot) {
    this.quotaSnapshot = Objects.requireNonNull(snapshot);
  }

  @Override
  public SpaceQuotaSnapshot getQuotaSnapshot() {
    return this.quotaSnapshot;
  }

  @Override
  public void initialize(
      RegionServerServices rss, TableName tableName, SpaceQuotaSnapshot snapshot) {
    setRegionServerServices(rss);
    setTableName(tableName);
    setQuotaSnapshot(snapshot);
  }

  @Override
  public boolean areCompactionsDisabled() {
    return false;
  }

  /**
   * Computes the size of a single file on the filesystem. If the size cannot be computed for some
   * reason, a {@link SpaceLimitingException} is thrown, as the file may violate a quota. If the
   * provided path does not reference a file, an {@link IllegalArgumentException} is thrown.
   *
   * @param fs The FileSystem which the path refers to a file upon
   * @param path The path on the {@code fs} to a file whose size is being checked
   * @return The size in bytes of the file
   */
  long getFileSize(FileSystem fs, String path) throws SpaceLimitingException {
    final FileStatus status;
    try {
      status = fs.getFileStatus(new Path(Objects.requireNonNull(path)));
    } catch (IOException e) {
      throw new SpaceLimitingException(
          getPolicyName(), "Could not verify length of file to bulk load: " + path, e);
    }
    if (!status.isFile()) {
      throw new IllegalArgumentException(path + " is not a file.");
    }
    return status.getLen();
  }
}
