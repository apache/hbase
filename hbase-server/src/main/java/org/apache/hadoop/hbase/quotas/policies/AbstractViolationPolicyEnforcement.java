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
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.quotas.SpaceLimitingException;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

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
  public void initialize(RegionServerServices rss, TableName tableName, SpaceQuotaSnapshot snapshot) {
    setRegionServerServices(rss);
    setTableName(tableName);
    setQuotaSnapshot(snapshot);
  }

  @Override
  public boolean areCompactionsDisabled() {
    return false;
  }

  @Override
  public boolean shouldCheckBulkLoads() {
    // Reference check. The singleton is used when no quota exists to check against
    return SpaceQuotaSnapshot.getNoSuchSnapshot() != quotaSnapshot;
  }

  @Override
  public void checkBulkLoad(FileSystem fs, List<String> paths) throws SpaceLimitingException {
    // Compute the amount of space that could be used to save some arithmetic in the for-loop
    final long sizeAvailableForBulkLoads = quotaSnapshot.getLimit() - quotaSnapshot.getUsage();
    long size = 0L;
    for (String path : paths) {
      size += addSingleFile(fs, path);
      if (size > sizeAvailableForBulkLoads) {
        break;
      }
    }
    if (size > sizeAvailableForBulkLoads) {
      throw new SpaceLimitingException(getPolicyName(), "Bulk load of " + paths
          + " is disallowed because the file(s) exceed the limits of a space quota.");
    }
  }

  private long addSingleFile(FileSystem fs, String path) throws SpaceLimitingException {
    final FileStatus status;
    try {
      status = fs.getFileStatus(new Path(Objects.requireNonNull(path)));
    } catch (IOException e) {
      throw new SpaceLimitingException(getPolicyName(), "Could not verify length of file to bulk load", e);
    }
    if (!status.isFile()) {
      throw new IllegalArgumentException(path + " is not a file.");
    }
    return status.getLen();
  }
}
