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
import java.lang.reflect.InvocationTargetException;
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
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
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

    // Enable the policy on a test directory. Try writing ot it to ensure that HDFS allows it
    // This acts as a safeguard against topology issues (not enough nodes for policy, etc) and
    // anything else. This is otherwise hard to validate more directly.
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

  private static void checkAvailable(DistributedFileSystem dfs, String requestedPolicy)
    throws HBaseIOException {
    Collection<Object> policies;

    try {
      policies = callDfsMethod(dfs, "getAllErasureCodingPolicies");
    } catch (IOException e) {
      throw new HBaseIOException("Failed to check for Erasure Coding policy: " + requestedPolicy,
        e);
    }
    for (Object policyInfo : policies) {
      if (checkPolicyMatch(policyInfo, requestedPolicy)) {
        return;
      }
    }
    throw new DoNotRetryIOException("Cannot set Erasure Coding policy: " + requestedPolicy
      + ". Policy not found. Available policies are: " + getPolicyNames(policies));
  }

  private static boolean checkPolicyMatch(Object policyInfo, String requestedPolicy)
    throws DoNotRetryIOException {
    try {
      String policyName = getPolicyNameFromInfo(policyInfo);
      if (requestedPolicy.equals(policyName)) {
        boolean isEnabled = callObjectMethod(policyInfo, "isEnabled");
        if (!isEnabled) {
          throw new DoNotRetryIOException("Cannot set Erasure Coding policy: " + requestedPolicy
            + ". The policy must be enabled, but has state "
            + callObjectMethod(policyInfo, "getState"));
        }
        return true;
      }
    } catch (DoNotRetryIOException e) {
      throw e;
    } catch (IOException e) {
      throw new DoNotRetryIOException(
        "Unable to check for match of Erasure Coding Policy " + policyInfo, e);
    }
    return false;
  }

  private static String getPolicyNameFromInfo(Object policyInfo) throws IOException {
    Object policy = callObjectMethod(policyInfo, "getPolicy");
    return callObjectMethod(policy, "getName");
  }

  private static String getPolicyNames(Collection<Object> policyInfos) {
    return policyInfos.stream().map(p -> {
      try {
        return getPolicyNameFromInfo(p);
      } catch (IOException e) {
        LOG.warn("Could not extract policy name from {}", p, e);
        return "unknown";
      }
    }).collect(Collectors.joining(", "));
  }

  /**
   * Check if EC policy is different between two descriptors
   * @return true if a sync is necessary
   */
  public static boolean needsSync(TableDescriptor oldDescriptor, TableDescriptor newDescriptor) {
    String newPolicy = oldDescriptor.getErasureCodingPolicy();
    String oldPolicy = newDescriptor.getErasureCodingPolicy();
    return !Objects.equals(oldPolicy, newPolicy);
  }

  /**
   * Sync the EC policy state from the newDescriptor onto the FS for the table dir of the provided
   * table descriptor. If the policy is null, we will remove erasure coding from the FS for the
   * table dir. If it's non-null, we'll set it to that policy.
   * @param newDescriptor descriptor containing the policy and table name
   */
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
    callDfsMethod(getDfs(fs), "setErasureCodingPolicy", path, policy);
  }

  /**
   * Unsets any EC policy specified on the path.
   */
  public static void unsetPolicy(FileSystem fs, Path rootDir, TableName tableName)
    throws IOException {
    DistributedFileSystem dfs = getDfs(fs);
    Path path = CommonFSUtils.getTableDir(rootDir, tableName);
    if (getPolicyNameForPath(dfs, path) == null) {
      LOG.warn("No EC policy set for path {}, nothing to unset", path);
      return;
    }
    callDfsMethod(dfs, "unsetErasureCodingPolicy", path);
  }

  public static void enablePolicy(FileSystem fs, String policy) throws IOException {
    callDfsMethod(getDfs(fs), "enableErasureCodingPolicy", policy);
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

  public static String getPolicyNameForPath(DistributedFileSystem dfs, Path path)
    throws IOException {
    Object policy = callDfsMethod(dfs, "getErasureCodingPolicy", path);
    if (policy == null) {
      return null;
    }
    return callObjectMethod(policy, "getName");
  }

  private interface ThrowingObjectSupplier {
    Object run() throws IOException;
  }

  private static <T> T callDfsMethod(DistributedFileSystem dfs, String name, Object... params)
    throws IOException {
    return callObjectMethod(dfs, name, params);
  }

  private static <T> T callObjectMethod(Object object, String name, Object... params)
    throws IOException {
    return unwrapInvocationException(() -> ReflectionUtils.invokeMethod(object, name, params));
  }

  private static <T> T unwrapInvocationException(ThrowingObjectSupplier runnable)
    throws IOException {
    try {
      return (T) runnable.run();
    } catch (UnsupportedOperationException e) {
      if (e.getCause() instanceof InvocationTargetException) {
        Throwable cause = e.getCause().getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        } else if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
      }
      throw e;
    }
  }
}
