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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/*
 * The two tests in this class are separated from TestRSGroups
 *   so that the observer pre/post hook called checks are stable.
 * There are some tests from TestRSGroupsBase which are empty since
 *   they would be run by TestRSGroups.
 */
@Category({MediumTests.class})
public class TestRSGroups1 extends TestRSGroups {
  protected static final Log LOG = LogFactory.getLog(TestRSGroups1.class);

  @Test
  public void testNamespaceConstraint() throws Exception {
    String nsName = tablePrefix+"_foo";
    String groupName = tablePrefix+"_foo";
    LOG.info("testNamespaceConstraint");
    rsGroupAdmin.addRSGroup(groupName);
    assertTrue(observer.preAddRSGroupCalled);
    assertTrue(observer.postAddRSGroupCalled);

    admin.createNamespace(NamespaceDescriptor.create(nsName)
        .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, groupName)
        .build());
    //test removing a referenced group
    try {
      rsGroupAdmin.removeRSGroup(groupName);
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
    //test modify group
    //changing with the same name is fine
    admin.modifyNamespace(
        NamespaceDescriptor.create(nsName)
          .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, groupName)
          .build());
    String anotherGroup = tablePrefix+"_anotherGroup";
    rsGroupAdmin.addRSGroup(anotherGroup);
    //test add non-existent group
    admin.deleteNamespace(nsName);
    rsGroupAdmin.removeRSGroup(groupName);
    assertTrue(observer.preRemoveRSGroupCalled);
    assertTrue(observer.postRemoveRSGroupCalled);
    try {
      admin.createNamespace(NamespaceDescriptor.create(nsName)
          .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, "foo")
          .build());
      fail("Expected a constraint exception");
    } catch (IOException ex) {
    }
  }

  @Test
  public void testMoveServers() throws Exception {
    super.testMoveServers();
    assertTrue(observer.preMoveServersCalled);
    assertTrue(observer.postMoveServersCalled);
  }

  @Ignore @Test
  public void testBogusArgs() throws Exception {
  }
  @Ignore @Test
  public void testCreateMultiRegion() throws IOException {
  }
  @Ignore @Test
  public void testCreateAndDrop() throws Exception {
  }
  @Ignore @Test
  public void testSimpleRegionServerMove() throws IOException,
      InterruptedException {
  }
  @Ignore @Test
  public void testTableMoveTruncateAndDrop() throws Exception {
  }
  @Ignore @Test
  public void testGroupBalance() throws Exception {
  }
  @Ignore @Test
  public void testRegionMove() throws Exception {
  }
  @Ignore @Test
  public void testFailRemoveGroup() throws IOException, InterruptedException {
  }
  @Ignore @Test
  public void testKillRS() throws Exception {
  }
  @Ignore @Test
  public void testValidGroupNames() throws IOException {
  }
  @Ignore @Test
  public void testMultiTableMove() throws Exception {
  }
  @Ignore @Test
  public void testMoveServersAndTables() throws Exception {
  }
  @Ignore @Test
  public void testDisabledTableMove() throws Exception {
  }
  @Ignore @Test
  public void testClearDeadServers() throws Exception {
  }
  @Ignore @Test
  public void testRemoveServers() throws Exception {
  }
  @Ignore @Test
  public void testBasicStartUp() throws IOException {
  }
  @Ignore @Test
  public void testNamespaceCreateAndAssign() throws Exception {
  }
  @Ignore @Test
  public void testDefaultNamespaceCreateAndAssign() throws Exception {
  }
  @Ignore @Test
  public void testGroupInfoMultiAccessing() throws Exception {
  }
  @Ignore @Test
  public void testMisplacedRegions() throws Exception {
  }
  @Ignore @Test
  public void testRSGroupBalancerSwitch() throws IOException {
  }
  @Ignore @Test
  public void testCloneSnapshot() throws Exception {
  }
}
