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
package org.apache.hadoop.hbase.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TestSecurityManager.SystemExitInTestException;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(RestTests.TAG)
@Tag(SmallTests.TAG)
public class TestRESTServerCommandLine {

  @Test
  public void testDaemonArguments() {
    Configuration conf = new Configuration(false);
    RESTServer.parseCommandLine(new String[] { "-Dhbase.regionserver.port=16020",
      "-Dhbase.regionserver.info.port=16030", "start" }, conf);

    assertEquals(16020, conf.getInt("hbase.regionserver.port", -1));
    assertEquals(16030, conf.getInt("hbase.regionserver.info.port", -1));
  }

  @Test
  public void testConfigurationProperties() {
    Configuration conf = new Configuration(false);
    conf.setInt("hbase.rest.port", 8080);
    RESTServer.parseCommandLine(new String[] { "-Dhbase.rest.port=18080",
      "-Dhbase.rest.info.port=-1", "-Dhbase.rest.readonly=true", "start" }, conf);

    assertEquals(18080, conf.getInt("hbase.rest.port", -1));
    assertEquals(-1, conf.getInt("hbase.rest.info.port", 0));
    assertTrue(conf.getBoolean("hbase.rest.readonly", false));
  }

  @Test
  public void testPropertiesWithRestOptions() {
    Configuration conf = new Configuration(false);
    RESTServer.parseCommandLine(
      new String[] { "-p", "18080", "-i", "18085", "-ro", "-Dhbase.regionserver.port=16020",
        "-Dhbase.regionserver.info.port=16030", "-Dhbase.rest.port=18081",
        "-Dhbase.rest.info.port=18086", "-Dhbase.rest.readonly=false", "start" },
      conf);

    assertEquals(16020, conf.getInt("hbase.regionserver.port", -1));
    assertEquals(16030, conf.getInt("hbase.regionserver.info.port", -1));
    assertEquals(18080, conf.getInt("hbase.rest.port", -1));
    assertEquals(18085, conf.getInt("hbase.rest.info.port", -1));
    assertTrue(conf.getBoolean("hbase.rest.readonly", false));
  }

  @Test
  public void testOptionsAfterStart() {
    Configuration conf = new Configuration(false);
    RESTServer.parseCommandLine(new String[] { "start", "-Dhbase.rest.port=18081", "--port",
      "18080", "--infoport", "18085", "--readonly" }, conf);

    assertEquals(18080, conf.getInt("hbase.rest.port", -1));
    assertEquals(18085, conf.getInt("hbase.rest.info.port", -1));
    assertTrue(conf.getBoolean("hbase.rest.readonly", false));
  }

  @Test
  public void testSeparatedAndRepeatedProperties() {
    Configuration conf = new Configuration(false);
    RESTServer.parseCommandLine(new String[] { "-D", "hbase.rest.test.value=first",
      "-Dhbase.rest.test.value=second=value", "start" }, conf);

    assertEquals("second=value", conf.get("hbase.rest.test.value"));
  }

  @Test
  public void testWithoutProperties() {
    Configuration conf = new Configuration(false);
    conf.setInt("hbase.rest.port", 18080);
    conf.setInt("hbase.rest.info.port", 18085);
    conf.setBoolean("hbase.rest.readonly", true);
    RESTServer.parseCommandLine(new String[] { "start" }, conf);

    assertEquals(18080, conf.getInt("hbase.rest.port", -1));
    assertEquals(18085, conf.getInt("hbase.rest.info.port", -1));
    assertTrue(conf.getBoolean("hbase.rest.readonly", false));
  }

  @Test
  public void testUnknownOption() {
    assertThrows(SystemExitInTestException.class, () -> RESTServer
      .parseCommandLine(new String[] { "--unknown", "start" }, new Configuration(false)));
  }

  @Test
  public void testMissingPropertyValue() {
    assertThrows(SystemExitInTestException.class,
      () -> RESTServer.parseCommandLine(new String[] { "start", "-D" }, new Configuration(false)));
  }

  @Test
  public void testExtraArgument() {
    assertThrows(SystemExitInTestException.class, () -> RESTServer
      .parseCommandLine(new String[] { "start", "extra" }, new Configuration(false)));
  }
}
