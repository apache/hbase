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
package org.apache.hadoop.hbase.chaos.actions;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Configuration common across the HDFS Actions.
 */
public final class HdfsActionUtils {

  private HdfsActionUtils() {
  }

  /**
   * Specify a user as whom HDFS actions should be run. The chaos process must have permissions
   * sufficient to assume the role of the specified user.
   * @see <a href=
   *      "https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/Superusers.html">Proxy
   *      user - Superusers Acting On Behalf Of Other Users</a>
   */
  public static final String HDFS_USER_CONF_KEY = "org.apache.hadoop.hbase.chaos.actions.hdfs_user";

  private static DistributedFileSystem createUnproxiedDfs(final Configuration conf)
    throws IOException {
    final Path rootDir = CommonFSUtils.getRootDir(conf);
    final FileSystem fs = rootDir.getFileSystem(conf);
    return (DistributedFileSystem) fs;
  }

  /**
   * Create an instance of {@link DistributedFileSystem} that honors {@value HDFS_USER_CONF_KEY}.
   */
  static DistributedFileSystem createDfs(final Configuration conf) throws IOException {
    final String proxyUser = conf.get(HDFS_USER_CONF_KEY);
    if (proxyUser == null) {
      return createUnproxiedDfs(conf);
    }
    final UserGroupInformation proxyUgi =
      UserGroupInformation.createProxyUser(proxyUser, UserGroupInformation.getLoginUser());
    try {
      return proxyUgi
        .doAs((PrivilegedExceptionAction<DistributedFileSystem>) () -> createUnproxiedDfs(conf));
    } catch (InterruptedException e) {
      final InterruptedIOException iioe = new InterruptedIOException(e.getMessage());
      iioe.setStackTrace(e.getStackTrace());
      throw iioe;
    }
  }
}
