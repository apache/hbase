/**
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
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestZooKeeperMainServerArg {
  private final ZooKeeperMainServerArg parser = new ZooKeeperMainServerArg();

  @Test public void test() {
    Configuration c = HBaseConfiguration.create();
    assertEquals("localhost:" + c.get(HConstants.ZOOKEEPER_CLIENT_PORT),
      parser.parse(c));
    final String port = "1234";
    c.set(HConstants.ZOOKEEPER_CLIENT_PORT, port);
    c.set("hbase.zookeeper.quorum", "example.com");
    assertEquals("example.com:" + port, parser.parse(c));
    c.set("hbase.zookeeper.quorum", "example1.com,example2.com,example3.com");
    assertTrue(port,
        parser.parse(c).matches("(example[1-3]\\.com,){2}example[1-3]\\.com:" + port));
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

