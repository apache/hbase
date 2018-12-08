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

package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master observer for restricting coprocessor assignments.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class CoprocessorWhitelistMasterObserver implements MasterCoprocessor, MasterObserver {

  public static final String CP_COPROCESSOR_WHITELIST_PATHS_KEY =
      "hbase.coprocessor.region.whitelist.paths";

  private static final Logger LOG = LoggerFactory
      .getLogger(CoprocessorWhitelistMasterObserver.class);

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public TableDescriptor preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, TableDescriptor currentDesc, TableDescriptor newDesc)
      throws IOException {
    verifyCoprocessors(ctx, newDesc);
    return newDesc;
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableDescriptor htd, RegionInfo[] regions) throws IOException {
    verifyCoprocessors(ctx, htd);
  }

  /**
   * Validates a single whitelist path against the coprocessor path
   * @param  coprocPath the path to the coprocessor including scheme
   * @param  wlPath     can be:
   *                      1) a "*" to wildcard all coprocessor paths
   *                      2) a specific filesystem (e.g. hdfs://my-cluster/)
   *                      3) a wildcard path to be evaluated by
   *                         {@link FilenameUtils#wildcardMatch(String, String)}
   *                         path can specify scheme or not (e.g.
   *                         "file:///usr/hbase/coprocessors" or for all
   *                         filesystems "/usr/hbase/coprocessors")
   * @return             if the path was found under the wlPath
   */
  private static boolean validatePath(Path coprocPath, Path wlPath) {
    // verify if all are allowed
    if (wlPath.toString().equals("*")) {
      return(true);
    }

    // verify we are on the same filesystem if wlPath has a scheme
    if (!wlPath.isAbsoluteAndSchemeAuthorityNull()) {
      String wlPathScheme = wlPath.toUri().getScheme();
      String coprocPathScheme = coprocPath.toUri().getScheme();
      String wlPathHost = wlPath.toUri().getHost();
      String coprocPathHost = coprocPath.toUri().getHost();
      if (wlPathScheme != null) {
        wlPathScheme = wlPathScheme.toString().toLowerCase();
      } else {
        wlPathScheme = "";
      }
      if (wlPathHost != null) {
        wlPathHost = wlPathHost.toString().toLowerCase();
      } else {
        wlPathHost = "";
      }
      if (coprocPathScheme != null) {
        coprocPathScheme = coprocPathScheme.toString().toLowerCase();
      } else {
        coprocPathScheme = "";
      }
      if (coprocPathHost != null) {
        coprocPathHost = coprocPathHost.toString().toLowerCase();
      } else {
        coprocPathHost = "";
      }
      if (!wlPathScheme.equals(coprocPathScheme) || !wlPathHost.equals(coprocPathHost)) {
        return(false);
      }
    }

    // allow any on this file-system (file systems were verified to be the same above)
    if (wlPath.isRoot()) {
      return(true);
    }

    // allow "loose" matches stripping scheme
    if (FilenameUtils.wildcardMatch(
        Path.getPathWithoutSchemeAndAuthority(coprocPath).toString(),
        Path.getPathWithoutSchemeAndAuthority(wlPath).toString())) {
      return(true);
    }
    return(false);
  }

  /**
   * Perform the validation checks for a coprocessor to determine if the path
   * is white listed or not.
   * @throws IOException if path is not included in whitelist or a failure
   *                     occurs in processing
   * @param  ctx         as passed in from the coprocessor
   * @param  htd         as passed in from the coprocessor
   */
  private static void verifyCoprocessors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableDescriptor htd) throws IOException {
    Collection<String> paths =
      ctx.getEnvironment().getConfiguration().getStringCollection(
            CP_COPROCESSOR_WHITELIST_PATHS_KEY);
    for (CoprocessorDescriptor cp : htd.getCoprocessorDescriptors()) {
      if (cp.getJarPath().isPresent()) {
        if (paths.stream().noneMatch(p -> {
          Path wlPath = new Path(p);
          if (validatePath(new Path(cp.getJarPath().get()), wlPath)) {
            LOG.debug(String.format("Coprocessor %s found in directory %s",
              cp.getClassName(), p));
            return true;
          }
          return false;
        })) {
          throw new IOException(String.format("Loading %s DENIED in %s",
            cp.getClassName(), CP_COPROCESSOR_WHITELIST_PATHS_KEY));
        }
      }
    }
  }
}
