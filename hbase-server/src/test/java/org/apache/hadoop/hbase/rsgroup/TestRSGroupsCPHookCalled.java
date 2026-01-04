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
package org.apache.hadoop.hbase.rsgroup;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RSGroupTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RSGroupTests.class, MediumTests.class })
public class TestRSGroupsCPHookCalled extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSGroupsCPHookCalled.class);

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
  public void testGetRSGroupInfoCPHookCalled() throws Exception {
    ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
    assertTrue(OBSERVER.preGetRSGroupInfoCalled);
    assertTrue(OBSERVER.postGetRSGroupInfoCalled);
  }

  @Test
  public void testGetRSGroupInfoOfTableCPHookCalled() throws Exception {
    ADMIN.getRSGroup(MetaTableName.getInstance());
    assertTrue(OBSERVER.preGetRSGroupInfoOfTableCalled);
    assertTrue(OBSERVER.postGetRSGroupInfoOfTableCalled);
  }

  @Test
  public void testListRSGroupsCPHookCalled() throws Exception {
    ADMIN.listRSGroups();
    assertTrue(OBSERVER.preListRSGroupsCalled);
    assertTrue(OBSERVER.postListRSGroupsCalled);
  }

  @Test
  public void testGetRSGroupInfoOfServerCPHookCalled() throws Exception {
    ServerName masterServerName = ((SingleProcessHBaseCluster) CLUSTER).getMaster().getServerName();
    ADMIN.getRSGroup(masterServerName.getAddress());
    assertTrue(OBSERVER.preGetRSGroupInfoOfServerCalled);
    assertTrue(OBSERVER.postGetRSGroupInfoOfServerCalled);
  }
}
