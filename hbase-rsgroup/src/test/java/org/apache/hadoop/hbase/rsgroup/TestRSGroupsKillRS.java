/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.rsgroup;

import static org.junit.Assert.assertFalse;

import com.google.common.collect.Sets;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class})
public class TestRSGroupsKillRS extends TestRSGroupsBase {
  protected static final Log LOG = LogFactory.getLog(TestRSGroupsKillRS.class);

  @BeforeClass
  public static void setUp() throws Exception {
    setUpTestBeforeClass();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    tearDownAfterClass();
  }

  @Before
  public void beforeMethod() throws Exception {
    setUpBeforeMethod();
  }

  @After
  public void afterMethod() throws Exception {
    tearDownAfterMethod();
  }

  @Test
  public void testKillRS() throws Exception {
    LOG.info("testKillRS");
    RSGroupInfo appInfo = addGroup(rsGroupAdmin, "appInfo", 1);

    final TableName tableName = TableName.valueOf(tablePrefix+"_ns", "_testKillRS");
    admin.createNamespace(
        NamespaceDescriptor.create(tableName.getNamespaceAsString())
            .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, appInfo.getName()).build());
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    admin.createTable(desc);
    //wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });

    ServerName targetServer = ServerName.parseServerName(
        appInfo.getServers().iterator().next().toString());
    AdminProtos.AdminService.BlockingInterface targetRS =
        admin.getConnection().getAdmin(targetServer);
    Assert.assertEquals(1, ProtobufUtil.getOnlineRegions(targetRS).size());

    try {
      //stopping may cause an exception
      //due to the connection loss
      targetRS.stopServer(null,
          AdminProtos.StopServerRequest.newBuilder().setReason("Die").build());
    } catch(Exception e) {
    }
    assertFalse(cluster.getClusterStatus().getServers().contains(targetServer));

    //wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return cluster.getClusterStatus().getRegionsInTransition().size() == 0;
      }
    });
    Set<Address> newServers = Sets.newHashSet();
    newServers.add(
        rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().iterator().next());
    rsGroupAdmin.moveServers(newServers, appInfo.getName());

    //Make sure all the table's regions get reassigned
    //disabling the table guarantees no conflicting assign/unassign (ie SSH) happens
    admin.disableTable(tableName);
    admin.enableTable(tableName);

    //wait for region to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return cluster.getClusterStatus().getRegionsInTransition().size() == 0;
      }
    });

    targetServer = ServerName.parseServerName(
        newServers.iterator().next().toString());
    targetRS =
        admin.getConnection().getAdmin(targetServer);
    Assert.assertEquals(1, ProtobufUtil.getOnlineRegions(targetRS).size());
    Assert.assertEquals(tableName,
        ProtobufUtil.getOnlineRegions(targetRS).get(0).getTable());
  }

}