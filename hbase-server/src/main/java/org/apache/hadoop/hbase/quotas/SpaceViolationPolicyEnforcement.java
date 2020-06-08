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
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * RegionServer implementation of {@link SpaceViolationPolicy}.
 *
 * Implementations must have a public, no-args constructor.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface SpaceViolationPolicyEnforcement {

  /**
   * Initializes this policy instance.
   */
  void initialize(RegionServerServices rss, TableName tableName, SpaceQuotaSnapshot snapshot);

  /**
   * Enables this policy. Not all policies have enable actions.
   */
  void enable() throws IOException;

  /**
   * Disables this policy. Not all policies have disable actions.
   */
  void disable() throws IOException;

  /**
   * Checks the given {@link Mutation} against <code>this</code> policy. If the
   * {@link Mutation} violates the policy, this policy should throw a
   * {@link SpaceLimitingException}.
   *
   * @throws SpaceLimitingException When the given mutation violates this policy.
   */
  void check(Mutation m) throws SpaceLimitingException;

  /**
   * Returns a logical name for the {@link SpaceViolationPolicy} that this enforcement is for.
   */
  String getPolicyName();

  /**
   * Returns whether or not compactions on this table should be disabled for this policy.
   */
  boolean areCompactionsDisabled();

  /**
   * Returns the {@link SpaceQuotaSnapshot} <code>this</code> was initialized with.
   */
  SpaceQuotaSnapshot getQuotaSnapshot();

  /**
   * Returns whether thet caller should verify any bulk loads against <code>this</code>.
   */
  boolean shouldCheckBulkLoads();

  /**
   * Computes the size of the file(s) at the given path against <code>this</code> policy and the
   * current {@link SpaceQuotaSnapshot}. If the file would violate the policy, a
   * {@link SpaceLimitingException} will be thrown.
   *
   * @param paths The paths in HDFS to files to be bulk loaded.
   * @return The size, in bytes, of the files that would be loaded.
   */
  long computeBulkLoadSize(FileSystem fs, List<String> paths) throws SpaceLimitingException;

}
