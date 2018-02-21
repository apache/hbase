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

import org.apache.hadoop.fs.FileSystem;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.quotas.SpaceLimitingException;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicyEnforcement;

/**
 * The default implementation for {@link SpaceViolationPolicyEnforcement}. This is done because all
 * tables, whether or not they're in violation now, should be checking bulk loads to proactively
 * catch a swell of files that would push the table into violation.
 */
@InterfaceAudience.Private
public class DefaultViolationPolicyEnforcement extends AbstractViolationPolicyEnforcement {

  @Override
  public void enable() throws IOException {}

  @Override
  public void disable() throws IOException {}

  @Override
  public String getPolicyName() {
    return "BulkLoadVerifying";
  }

  @Override
  public void check(Mutation m) throws SpaceLimitingException {}

  @Override
  public boolean shouldCheckBulkLoads() {
    // Reference check. The singleton is used when no quota exists to check against
    return SpaceQuotaSnapshot.getNoSuchSnapshot() != quotaSnapshot;
  }

  @Override
  public long computeBulkLoadSize(FileSystem fs, List<String> paths) throws SpaceLimitingException {
    // Compute the amount of space that could be used to save some arithmetic in the for-loop
    final long sizeAvailableForBulkLoads = quotaSnapshot.getLimit() - quotaSnapshot.getUsage();
    long size = 0L;
    for (String path : paths) {
      try {
        size += getFileSize(fs, path);
      } catch (IOException e) {
        throw new SpaceLimitingException(
            getPolicyName(), "Colud not verify length of file to bulk load: " + path, e);
      }
      if (size > sizeAvailableForBulkLoads) {
        throw new SpaceLimitingException(getPolicyName(), "Bulk load of " + paths
            + " is disallowed because the file(s) exceed the limits of a space quota.");
      }
    }
    return size;
  }
}
