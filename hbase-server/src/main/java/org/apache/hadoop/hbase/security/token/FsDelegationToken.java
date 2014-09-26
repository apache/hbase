/**
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

package org.apache.hadoop.hbase.security.token;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.security.token.Token;

/**
 * Helper class to obtain a filesystem delegation token.
 * Mainly used by Map-Reduce jobs that requires to read/write data to 
 * a remote file-system (e.g. BulkLoad, ExportSnapshot).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FsDelegationToken {
  private static final Log LOG = LogFactory.getLog(FsDelegationToken.class);

  private final UserProvider userProvider;
  private final String renewer;

  private boolean hasForwardedToken = false;
  private Token<?> userToken = null;
  private FileSystem fs = null;

  /*
   * @param renewer the account name that is allowed to renew the token.
   */
  public FsDelegationToken(final UserProvider userProvider, final String renewer) {
    this.userProvider = userProvider;
    this.renewer = renewer;
  }

  /**
   * Acquire the delegation token for the specified filesytem.
   * Before requesting a new delegation token, tries to find one already available.
   *
   * @param fs the filesystem that requires the delegation token
   * @throws IOException on fs.getDelegationToken() failure
   */
  public void acquireDelegationToken(final FileSystem fs)
      throws IOException {
    if (userProvider.isHadoopSecurityEnabled()) {
      this.fs = fs;
      userToken = userProvider.getCurrent().getToken("HDFS_DELEGATION_TOKEN",
                                                      fs.getCanonicalServiceName());
      if (userToken == null) {
        hasForwardedToken = false;
        try {
          userToken = fs.getDelegationToken(renewer);
        } catch (NullPointerException npe) {
          // we need to handle NullPointerException in case HADOOP-10009 is missing
          LOG.error("Failed to get token for " + renewer);
        }
      } else {
        hasForwardedToken = true;
        LOG.info("Use the existing token: " + userToken);
      }
    }
  }

  /**
   * Releases a previously acquired delegation token.
   */
  public void releaseDelegationToken() {
    if (userProvider.isHadoopSecurityEnabled()) {
      if (userToken != null && !hasForwardedToken) {
        try {
          userToken.cancel(this.fs.getConf());
        } catch (Exception e) {
          LOG.warn("Failed to cancel HDFS delegation token: " + userToken, e);
        }
      }
      this.userToken = null;
      this.fs = null;
    }
  }

  public UserProvider getUserProvider() {
    return userProvider;
  }

  /**
   * @return the account name that is allowed to renew the token.
   */
  public String getRenewer() {
    return renewer;
  }

  /**
   * @return the delegation token acquired, or null in case it was not acquired
   */
  public Token<?> getUserToken() {
    return userToken;
  }

  public FileSystem getFileSystem() {
    return fs;
  }
}
