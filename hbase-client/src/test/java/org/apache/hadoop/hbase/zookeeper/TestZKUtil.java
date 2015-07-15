/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *
 */
@Category({SmallTests.class})
public class TestZKUtil {

  @Test
  public void testGetZooKeeperClusterKey() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM, "\tlocalhost\n");
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "3333");
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "hbase");
    String clusterKey = ZKUtil.getZooKeeperClusterKey(conf, "test");
    Assert.assertTrue(!clusterKey.contains("\t") && !clusterKey.contains("\n"));
    Assert.assertEquals("localhost:3333:hbase,test", clusterKey);
  }
}
