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
package org.apache.hadoop.hbase.rsgroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestRSGroupConfig extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRSGroupConfig.class);

  @Rule
  public TestName name = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestRSGroupConfig.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestRSGroupsBase.setUpTestBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TestRSGroupsBase.tearDownAfterClass();
  }

  @Test
  public void testSetDefaultGroupConfiguration() {
    assertThrows(ConstraintException.class, () -> testSetConfiguration(RSGroupInfo.DEFAULT_GROUP));
  }

  @Test
  public void testSetNonDefaultGroupConfiguration() throws IOException {
    String group = getGroupName(name.getMethodName());
    rsGroupAdmin.addRSGroup(group);
    testSetConfiguration(group);
    rsGroupAdmin.removeRSGroup(group);
  }

  private void testSetConfiguration(String group) throws IOException {
    Map<String, String> configuration = new HashMap<>();
    configuration.put("aaa", "111");
    configuration.put("bbb", "222");
    rsGroupAdmin.updateRSGroupConfig(group, configuration);
    RSGroupInfo rsGroup = rsGroupAdmin.getRSGroupInfo(group);
    assertEquals(configuration, rsGroup.getConfiguration());
    // unset configuration
    rsGroupAdmin.updateRSGroupConfig(group, null);
    rsGroup = rsGroupAdmin.getRSGroupInfo(group);
    Map<String, String> configFromGroup = rsGroup.getConfiguration();
    assertNotNull(configFromGroup);
    assertEquals(0, configFromGroup.size());
  }

}