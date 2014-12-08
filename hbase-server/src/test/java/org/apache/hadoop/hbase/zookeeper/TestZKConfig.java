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
package org.apache.hadoop.hbase.zookeeper;

import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestZKConfig {
  @Test
  public void testZKConfigLoading() throws Exception {
    // Test depends on test resource 'zoo.cfg' at src/test/resources/zoo.cfg
    Configuration conf = HBaseConfiguration.create();
    // Test that by default we do not pick up any property from the zoo.cfg
    // since that feature is to be deprecated and removed. So we should read only
    // from the config instance (i.e. via hbase-default.xml and hbase-site.xml)
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
    Properties props = ZKConfig.makeZKProps(conf);
    Assert.assertEquals(
        "Property client port should have been default from the HBase config",
        "2181",
        props.getProperty("clientPort"));
    // Test deprecated zoo.cfg read support by explicitly enabling it and
    // thereby relying on our test resource zoo.cfg to be read.
    // We may remove this test after a higher release (i.e. post-deprecation).
    conf.setBoolean(HConstants.HBASE_CONFIG_READ_ZOOKEEPER_CONFIG, true);
    props = ZKConfig.makeZKProps(conf);
    Assert.assertEquals(
        "Property client port should have been from zoo.cfg",
        "9999",
        props.getProperty("clientPort"));
  }
}
