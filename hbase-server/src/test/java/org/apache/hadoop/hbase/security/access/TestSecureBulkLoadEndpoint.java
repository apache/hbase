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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the SecureBulkLoadEndpoint code.
 */
@Category(SmallTests.class)
public class TestSecureBulkLoadEndpoint {

  @Test
  public void testFileSystemsWithoutPermissionSupport() {
    final Configuration emptyConf = new Configuration(false);
    final Configuration defaultConf = HBaseConfiguration.create();

    final Set<String> expectedDefaultIgnoredSchemes = new HashSet<>(
        Arrays.asList(
          StringUtils.split(SecureBulkLoadEndpoint.FS_WITHOUT_SUPPORT_PERMISSION_DEFAULT, ',')));

    final SecureBulkLoadEndpoint endpoint = new SecureBulkLoadEndpoint();

    // Empty configuration should return the default list of schemes
    Set<String> defaultIgnoredSchemes = endpoint.getFileSystemSchemesWithoutPermissionSupport(
        emptyConf);
    assertEquals(defaultIgnoredSchemes, expectedDefaultIgnoredSchemes);

    // Default configuration (unset) should be the default list of schemes
    defaultIgnoredSchemes = endpoint.getFileSystemSchemesWithoutPermissionSupport(defaultConf);
    assertEquals(defaultIgnoredSchemes, expectedDefaultIgnoredSchemes);

    defaultConf.set(SecureBulkLoadEndpoint.FS_WITHOUT_SUPPORT_PERMISSION_KEY, "foo,bar");
    defaultIgnoredSchemes = endpoint.getFileSystemSchemesWithoutPermissionSupport(defaultConf);
    assertEquals(defaultIgnoredSchemes, new HashSet<String>(Arrays.asList("foo", "bar")));
  }
}
