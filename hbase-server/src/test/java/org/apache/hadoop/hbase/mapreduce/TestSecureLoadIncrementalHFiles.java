/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * Reruns TestLoadIncrementalHFiles using LoadIncrementalHFiles in secure mode.
 * This suite is unable to verify the security handoff/turnover
 * as miniCluster is running as system user thus has root privileges
 * and delegation tokens don't seem to work on miniDFS.
 *
 * Thus SecureBulkload can only be completely verified by running
 * integration tests against a secure cluster. This suite is still
 * invaluable as it verifies the other mechanisms that need to be
 * supported as part of a LoadIncrementalFiles call.
 */
@Category(LargeTests.class)
public class TestSecureLoadIncrementalHFiles extends  TestLoadIncrementalHFiles{

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // set the always on security provider
    UserProvider.setUserProviderForTesting(util.getConfiguration(),
      HadoopSecurityEnabledUserProviderForTesting.class);
    // setup configuration
    SecureTestUtil.enableSecurity(util.getConfiguration());
    util.getConfiguration().setInt(
        LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY,
        MAX_FILES_PER_REGION_PER_FAMILY);

    util.startMiniCluster();

    // Wait for the ACL table to become available
    util.waitTableEnabled(AccessControlLists.ACL_TABLE_NAME);

    setupNamespace();
  }

}

