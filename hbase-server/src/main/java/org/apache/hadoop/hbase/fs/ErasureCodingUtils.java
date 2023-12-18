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
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class ErasureCodingUtils {

  private ErasureCodingUtils() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(ErasureCodingUtils.class);

  /**
   * Runs checks against the FileSystem, verifying that HDFS is supported and the policy is
   * available, enabled, and works with a simple write.
   */
  public static void verifySupport(Configuration conf, String policy) throws HBaseIOException {
    DistributedFileSystem dfs = getDfs(conf);
    checkAvailable(dfs, policy);

    Path globalTempDir = new Path(conf.get(HConstants.HBASE_DIR), HConstants.HBASE_TEMP_DIRECTORY);
    Path currentTempDir = createTempDir(dfs, globalTempDir);
    try {
      setPolicy(dfs, currentTempDir, policy);
      try (FSDataOutputStream out = dfs.create(new Path(currentTempDir, "test.out"))) {
        out.writeUTF("Testing " + policy);
      }
    } catch (IOException e) {
      throw new DoNotRetryIOException("Failed write test for EC policy. Check cause or logs", e);
    } finally {
      try {
        dfs.delete(currentTempDir, true);
      } catch (IOException e) {
        LOG.warn("Failed to delete temp path for ec test", e);
      }
    }
  }

  private static Path createTempDir(FileSystem fs, Path tempDir) throws HBaseIOException {
    Path currentTempDir = new Path(tempDir, "ec-test-" + System.currentTimeMillis());
    try {
      fs.mkdirs(currentTempDir);
      fs.deleteOnExit(currentTempDir);
    } catch (IOException e) {
      throw new HBaseIOException("Failed to create test dir for EC write test", e);
    }
    return currentTempDir;
  }

  private static void checkAvailable(DistributedFileSystem dfs, String policy)
    throws HBaseIOException {
    Collection<ErasureCodingPolicyInfo> policies;
    try {
      policies = dfs.getAllErasureCodingPolicies();
    } catch (IOException e) {
      throw new HBaseIOException("Failed to check for Erasure Coding policy: " + policy, e);
    }
    for (ErasureCodingPolicyInfo policyInfo : policies) {
      if (policyInfo.getPolicy().getName().equals(policy)) {
        if (!policyInfo.isEnabled()) {
          throw new DoNotRetryIOException("Cannot set Erasure Coding policy: " + policy
            + ". The policy must be enabled, but has state " + policyInfo.getState());
        }
        return;
      }
    }
    throw new DoNotRetryIOException(
      "Cannot set Erasure Coding policy: " + policy + ". Policy not found. Available policies are: "
        + policies.stream().map(p -> p.getPolicy().getName()).collect(Collectors.joining(", ")));
  }

  public static boolean needsSync(TableDescriptor oldDescriptor, TableDescriptor newDescriptor) {
    String newPolicy = oldDescriptor.getErasureCodingPolicy();
    String oldPolicy = newDescriptor.getErasureCodingPolicy();
    return !Objects.equals(oldPolicy, newPolicy);
  }

  public static void sync(FileSystem fs, Path rootDir, TableDescriptor newDescriptor)
    throws IOException {
    String newPolicy = newDescriptor.getErasureCodingPolicy();
    if (newPolicy == null) {
      unsetPolicy(fs, rootDir, newDescriptor.getTableName());
    } else {
      setPolicy(fs, rootDir, newDescriptor.getTableName(), newPolicy);
    }
  }

  /**
   * Sets the EC policy on the table directory for the specified table
   */
  public static void setPolicy(FileSystem fs, Path rootDir, TableName tableName, String policy)
    throws IOException {
    Path path = CommonFSUtils.getTableDir(rootDir, tableName);
    setPolicy(fs, path, policy);
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
  public static void unsetPolicy(FileSystem fs, Path rootDir, TableName tableName)
    throws IOException {
    DistributedFileSystem dfs = getDfs(fs);
    Path path = CommonFSUtils.getTableDir(rootDir, tableName);
    if (dfs.getErasureCodingPolicy(path) == null) {
      LOG.warn("No EC policy set for path {}, nothing to unset", path);
      return;
    }
    dfs.unsetErasureCodingPolicy(path);
  }

  private static DistributedFileSystem getDfs(Configuration conf) throws HBaseIOException {
    try {
      return getDfs(FileSystem.get(conf));
    } catch (DoNotRetryIOException e) {
      throw e;
    } catch (IOException e) {
      throw new HBaseIOException("Failed to get FileSystem from conf", e);
    }

  }

  private static DistributedFileSystem getDfs(FileSystem fs) throws DoNotRetryIOException {
    if (!(fs instanceof DistributedFileSystem)) {
      throw new DoNotRetryIOException(
        "Cannot manage Erasure Coding policy. Erasure Coding is only available on HDFS, but fs is "
          + fs.getClass().getSimpleName());
    }
    return (DistributedFileSystem) fs;
  }
}
