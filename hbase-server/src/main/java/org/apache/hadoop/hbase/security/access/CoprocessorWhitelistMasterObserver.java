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
import java.net.URI;
import java.nio.file.PathMatcher;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Master observer for restricting coprocessor assignments.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class CoprocessorWhitelistMasterObserver extends BaseMasterObserver {

  public static final String CP_COPROCESSOR_WHITELIST_PATHS_KEY =
      "hbase.coprocessor.region.whitelist.paths";

  private static final Log LOG = LogFactory
      .getLog(CoprocessorWhitelistMasterObserver.class);

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HTableDescriptor htd) throws IOException {
    verifyCoprocessors(ctx, htd);
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor htd, HRegionInfo[] regions) throws IOException {
    verifyCoprocessors(ctx, htd);
  }

  /**
   * Validates a single whitelist path against the coprocessor path
   * @param  coprocPath the path to the coprocessor including scheme
   * @param  wlPath     can be:
   *                      1) a "*" to wildcard all coprocessor paths
   *                      2) a specific filesystem (e.g. hdfs://my-cluster/)
   *                      3) a wildcard path to be evaluated by
   *                         {@link FilenameUtils.wildcardMatch}
   *                         path can specify scheme or not (e.g.
   *                         "file:///usr/hbase/coprocessors" or for all
   *                         filesystems "/usr/hbase/coprocessors")
   * @return             if the path was found under the wlPath
   * @throws IOException if a failure occurs in getting the path file system
   */
  private static boolean validatePath(Path coprocPath, Path wlPath,
      Configuration conf) throws IOException {
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
  private void verifyCoprocessors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor htd) throws IOException {

    MasterServices services = ctx.getEnvironment().getMasterServices();
    Configuration conf = services.getConfiguration();

    Collection<String> paths =
        conf.getStringCollection(
            CP_COPROCESSOR_WHITELIST_PATHS_KEY);

    List<String> coprocs = htd.getCoprocessors();
    for (int i = 0; i < coprocs.size(); i++) {
      String coproc = coprocs.get(i);

      String coprocSpec = Bytes.toString(htd.getValue(
          Bytes.toBytes("coprocessor$" + (i + 1))));
      if (coprocSpec == null) {
        continue;
      }

      // File path is the 1st field of the coprocessor spec
      Matcher matcher =
          HConstants.CP_HTD_ATTR_VALUE_PATTERN.matcher(coprocSpec);
      if (matcher == null || !matcher.matches()) {
        continue;
      }

      String coprocPathStr = matcher.group(1).trim();
      // Check if coprocessor is being loaded via the classpath (i.e. no file path)
      if (coprocPathStr.equals("")) {
        break;
      }
      Path coprocPath = new Path(coprocPathStr);
      String coprocessorClass = matcher.group(2).trim();

      boolean foundPathMatch = false;
      for (String pathStr : paths) {
        Path wlPath = new Path(pathStr);
        try {
          foundPathMatch = validatePath(coprocPath, wlPath, conf);
          if (foundPathMatch == true) {
            LOG.debug(String.format("Coprocessor %s found in directory %s",
                coprocessorClass, pathStr));
            break;
          }
        } catch (IOException e) {
          LOG.warn(String.format("Failed to validate white list path %s for coprocessor path %s",
              pathStr, coprocPathStr));
        }
      }
      if (!foundPathMatch) {
        throw new IOException(String.format("Loading %s DENIED in %s",
            coprocessorClass, CP_COPROCESSOR_WHITELIST_PATHS_KEY));
      }
    }
  }
}
