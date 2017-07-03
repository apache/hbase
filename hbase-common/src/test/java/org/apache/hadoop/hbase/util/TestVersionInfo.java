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

import org.apache.hadoop.hbase.testclassification.SmallTests;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestVersionInfo {

  @Test
  public void testCompareVersion() {
    assertTrue(VersionInfo.compareVersion("1.0.0", "0.98.11") > 0);
    assertTrue(VersionInfo.compareVersion("0.98.11", "1.0.1") < 0);
    assertTrue(VersionInfo.compareVersion("2.0.0", "1.4.0") > 0);
    assertTrue(VersionInfo.compareVersion("2.0.0", "2.0.0-SNAPSHOT") < 0);
  }
}