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
package org.apache.hadoop.hbase.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This should be in the hbase-logging module but the {@link HBaseClassTestRule} is in hbase-common
 * so we can only put the class in hbase-common module for now...
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestLog4jUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLog4jUtils.class);

  @Test
  public void test() {
    org.apache.logging.log4j.Logger zk =
      org.apache.logging.log4j.LogManager.getLogger("org.apache.zookeeper");
    org.apache.logging.log4j.Level zkLevel = zk.getLevel();
    org.apache.logging.log4j.Logger hbaseZk =
      org.apache.logging.log4j.LogManager.getLogger("org.apache.hadoop.hbase.zookeeper");
    org.apache.logging.log4j.Level hbaseZkLevel = hbaseZk.getLevel();
    org.apache.logging.log4j.Logger client =
      org.apache.logging.log4j.LogManager.getLogger("org.apache.hadoop.hbase.client");
    org.apache.logging.log4j.Level clientLevel = client.getLevel();
    Log4jUtils.disableZkAndClientLoggers();
    assertEquals(org.apache.logging.log4j.Level.OFF, zk.getLevel());
    assertEquals(org.apache.logging.log4j.Level.OFF.toString(),
      Log4jUtils.getEffectiveLevel(zk.getName()));
    assertEquals(org.apache.logging.log4j.Level.OFF, hbaseZk.getLevel());
    assertEquals(org.apache.logging.log4j.Level.OFF.toString(),
      Log4jUtils.getEffectiveLevel(hbaseZk.getName()));
    assertEquals(org.apache.logging.log4j.Level.OFF, client.getLevel());
    assertEquals(org.apache.logging.log4j.Level.OFF.toString(),
      Log4jUtils.getEffectiveLevel(client.getName()));
    // restore the level
    org.apache.logging.log4j.core.config.Configurator.setLevel(zk.getName(), zkLevel);
    org.apache.logging.log4j.core.config.Configurator.setLevel(hbaseZk.getName(), hbaseZkLevel);
    org.apache.logging.log4j.core.config.Configurator.setLevel(client.getName(), clientLevel);
  }

  @Test
  public void testGetLogFiles() throws IOException {
    // we use console appender in tests so the active log files should be empty
    assertTrue(Log4jUtils.getActiveLogFiles().isEmpty());
  }
}
