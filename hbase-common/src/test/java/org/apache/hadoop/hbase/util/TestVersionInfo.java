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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestVersionInfo {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestVersionInfo.class);

  @Test
  public void testCompareVersion() {
    assertTrue(VersionInfo.compareVersion("1.0.0", "0.98.11") > 0);
    assertTrue(VersionInfo.compareVersion("0.98.11", "1.0.1") < 0);
    assertTrue(VersionInfo.compareVersion("2.0.0", "1.4.0") > 0);
    assertTrue(VersionInfo.compareVersion("2.0.0", "2.0.0-SNAPSHOT") > 0);
    assertTrue(VersionInfo.compareVersion("0.94.6.1", "0.96.1.1") < 0);
    assertTrue(VersionInfo.compareVersion("0.96.1.1", "0.98.6.1") < 0);
    assertTrue(VersionInfo.compareVersion("0.98.6.1", "0.98.10.1") < 0);
    assertTrue(VersionInfo.compareVersion("0.98.10.1", "0.98.12.1") < 0);
    assertTrue(VersionInfo.compareVersion("0.98.12.1", "0.98.16.1") < 0);
    assertTrue(VersionInfo.compareVersion("0.98.16.1", "1.0.1.1") < 0);
    assertTrue(VersionInfo.compareVersion("1.0.1.1", "1.1.0.1") < 0);
    assertTrue(VersionInfo.compareVersion("2.0..1", "2.0.0") > 0);
    assertTrue(VersionInfo.compareVersion("2.0.0", "2.0.0") == 0);
    assertTrue(VersionInfo.compareVersion("1.99.14", "2.0.0-alpha-1") < 0);
    assertTrue(VersionInfo.compareVersion("2.0.0-alpha-1", "2.0.0-beta-3") < 0);
    assertTrue(VersionInfo.compareVersion("2.0.0-beta-3", "2.0.0-SNAPSHOT") < 0);
    assertTrue(VersionInfo.compareVersion("2.0.0-SNAPSHOT", "2.0") < 0);
    assertTrue(VersionInfo.compareVersion("2.0", "2.0.0.1") < 0);
    assertTrue(VersionInfo.compareVersion("2.0.0.1", "2.0.1") < 0);
    assertTrue(VersionInfo.compareVersion("3.0.0-alpha-2", "3.0.0-alpha-11") < 0);
    assertTrue(VersionInfo.compareVersion("3.0.0-beta-2", "3.0.0-beta-11") < 0);
    assertTrue(VersionInfo.compareVersion("2.0.0-foobar", "2.0.0.1") < 0);
    assertTrue(VersionInfo.compareVersion("2.any.any", "2.0.0") < 0);
    assertTrue(VersionInfo.compareVersion("2.0.0", "2.any.any") > 0);
    assertTrue(VersionInfo.compareVersion("2.any.any", "2.0.0-alpha-1") < 0);
    assertTrue(VersionInfo.compareVersion("2.0.0-alpha-1", "2.any.any") > 0);
    assertTrue(VersionInfo.compareVersion("2.any.any", "2.0.0-beta-5-SNAPSHOT") < 0);
    assertTrue(VersionInfo.compareVersion("2.0.0-beta-5-SNAPSHOT", "2.any.any") > 0);
    assertTrue(VersionInfo.compareVersion("2.any.any", "1.4.4") > 0);
    assertTrue(VersionInfo.compareVersion("1.4.4", "2.any.any") < 0);
  }
}
