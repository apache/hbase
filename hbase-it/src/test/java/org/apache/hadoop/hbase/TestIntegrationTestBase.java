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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.chaos.factories.MonkeyConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestIntegrationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestIntegrationTestBase.class);

  @Test
  public void testMonkeyPropertiesParsing() {
    final Configuration conf = new Configuration(false);
    conf.set(MonkeyConstants.BATCH_RESTART_RS_RATIO, "0.85");
    conf.set(MonkeyConstants.MOVE_REGIONS_MAX_TIME, "60000");
    conf.set("hbase.rootdir", "/foo/bar/baz");

    final Properties props = new Properties();
    IntegrationTestBase testBase = new IntegrationTestDDLMasterFailover();
    assertEquals(0, props.size());
    testBase.loadMonkeyProperties(props, conf);
    assertEquals(2, props.size());
    assertEquals("0.85", props.getProperty(MonkeyConstants.BATCH_RESTART_RS_RATIO));
    assertEquals("60000", props.getProperty(MonkeyConstants.MOVE_REGIONS_MAX_TIME));
  }
}
