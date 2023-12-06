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
package org.apache.hadoop.hbase.fs;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class ErasureCodingUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ErasureCodingUtils.class);

  /**
   * Runs checks against the FileSystem, verifying that HDFS is supported and the policy is
   * available and enabled.
   */
  public static void checkAvailable(FileSystem fs, String policy) throws HBaseIOException {
    DistributedFileSystem dfs = getDfs(fs);

    Collection<ErasureCodingPolicyInfo> policies;
    try {
      policies = dfs.getAllErasureCodingPolicies();
    } catch (IOException e) {
      throw new HBaseIOException("Failed to check for Erasure Coding policy: " + policy, e);
    }

    for (ErasureCodingPolicyInfo policyInfo : policies) {
      if (policyInfo.getPolicy().getName().equals(policy)) {
        if (!policyInfo.isEnabled()) {
          throw new HBaseIOException("Cannot set Erasure Coding policy: " + policy
            + ". The policy must be enabled, but has state " + policyInfo.getState());
        }
        return;
      }
    }

    throw new HBaseIOException(
      "Cannot set Erasure Coding policy: " + policy + ". Policy not found. Available policies are: "
        + policies.stream().map(p -> p.getPolicy().getName()).collect(Collectors.joining(", ")));
  }

  /**
   * Sets the EC policy on the path
   */
  public static void setPolicy(FileSystem fs, Path path, String policy) throws IOException {
    getDfs(fs).setErasureCodingPolicy(path, policy);
  }

  /**
   * Unsets any EC policy specified on the path.
   */
  public static void unsetPolicy(FileSystem fs, Path path) throws IOException {
    DistributedFileSystem dfs = getDfs(fs);
    if (dfs.getErasureCodingPolicy(path) == null) {
      LOG.warn("No EC policy set for path {}, nothing to unset", path);
      return;
    }
    dfs.unsetErasureCodingPolicy(path);
  }

  private static DistributedFileSystem getDfs(FileSystem fs) throws HBaseIOException {
    if (!(fs instanceof DistributedFileSystem)) {
      throw new HBaseIOException(
        "Cannot manage Erasure Coding policy. Erasure Coding is only available on HDFS, but fs is "
          + fs.getClass().getSimpleName());
    }
    return (DistributedFileSystem) fs;
  }
}
