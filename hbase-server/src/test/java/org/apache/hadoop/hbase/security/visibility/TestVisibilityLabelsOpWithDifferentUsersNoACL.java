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
package org.apache.hadoop.hbase.security.visibility;

import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.GetAuthsResponse;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos.VisibilityLabelsResponse;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.protobuf.ByteString;

@Category(MediumTests.class)
public class TestVisibilityLabelsOpWithDifferentUsersNoACL {
  private static final String PRIVATE = "private";
  private static final String CONFIDENTIAL = "confidential";
  private static final String SECRET = "secret";
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  @Rule
  public final TestName TEST_NAME = new TestName();
  private static User SUPERUSER;
  private static User NORMAL_USER;
  private static User NORMAL_USER1;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // setup configuration
    conf = TEST_UTIL.getConfiguration();
    VisibilityTestUtil.enableVisiblityLabels(conf);
    String currentUser = User.getCurrent().getName();
    conf.set("hbase.superuser", "admin,"+currentUser);
    TEST_UTIL.startMiniCluster(2);

    // Wait for the labels table to become available
    TEST_UTIL.waitTableEnabled(LABELS_TABLE_NAME.getName(), 50000);
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    NORMAL_USER = User.createUserForTesting(conf, "user1", new String[] {});
    NORMAL_USER1 = User.createUserForTesting(conf, "user2", new String[] {});
    addLabels();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testLabelsTableOpsWithDifferentUsers() throws Throwable {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action =
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        try {
          return VisibilityClient.setAuths(conf, new String[] { CONFIDENTIAL, PRIVATE }, "user1");
        } catch (Throwable e) {
        }
        return null;
      }
    };
    VisibilityLabelsResponse response = SUPERUSER.runAs(action);
    assertTrue(response.getResult(0).getException().getValue().isEmpty());
    assertTrue(response.getResult(1).getException().getValue().isEmpty());
    
    // Ideally this should not be allowed.  this operation should fail or do nothing.
    action = new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        try {
          return VisibilityClient.setAuths(conf, new String[] { CONFIDENTIAL, PRIVATE }, "user3");
        } catch (Throwable e) {
        }
        return null;
      }
    };
    response = NORMAL_USER1.runAs(action);
    assertEquals("org.apache.hadoop.hbase.security.AccessDeniedException", response
        .getResult(0).getException().getName());
    assertEquals("org.apache.hadoop.hbase.security.AccessDeniedException", response
        .getResult(1).getException().getName());

    PrivilegedExceptionAction<GetAuthsResponse> action1 =
        new PrivilegedExceptionAction<GetAuthsResponse>() {
      public GetAuthsResponse run() throws Exception {
        try {
          return VisibilityClient.getAuths(conf, "user1");
        } catch (Throwable e) {
        }
        return null;
      }
    };
    GetAuthsResponse authsResponse = NORMAL_USER.runAs(action1);
    assertTrue(authsResponse.getAuthList().isEmpty());
    authsResponse = NORMAL_USER1.runAs(action1);
    assertTrue(authsResponse.getAuthList().isEmpty());
    authsResponse = SUPERUSER.runAs(action1);
    List<String> authsList = new ArrayList<String>();
    for (ByteString authBS : authsResponse.getAuthList()) {
      authsList.add(Bytes.toString(authBS.toByteArray()));
    }
    assertEquals(2, authsList.size());
    assertTrue(authsList.contains(CONFIDENTIAL));
    assertTrue(authsList.contains(PRIVATE));

    PrivilegedExceptionAction<VisibilityLabelsResponse> action2 = 
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        try {
          return VisibilityClient.clearAuths(conf, new String[] { CONFIDENTIAL, PRIVATE }, "user1");
        } catch (Throwable e) {
        }
        return null;
      }
    };
    response = NORMAL_USER1.runAs(action2);
    assertEquals("org.apache.hadoop.hbase.security.AccessDeniedException", response
        .getResult(0).getException().getName());
    assertEquals("org.apache.hadoop.hbase.security.AccessDeniedException", response
        .getResult(1).getException().getName());
    response = SUPERUSER.runAs(action2);
    assertTrue(response.getResult(0).getException().getValue().isEmpty());
    assertTrue(response.getResult(1).getException().getValue().isEmpty());
    authsResponse = SUPERUSER.runAs(action1);
    assertTrue(authsResponse.getAuthList().isEmpty());
  }

  private static void addLabels() throws Exception {
    PrivilegedExceptionAction<VisibilityLabelsResponse> action = 
        new PrivilegedExceptionAction<VisibilityLabelsResponse>() {
      public VisibilityLabelsResponse run() throws Exception {
        String[] labels = { SECRET, CONFIDENTIAL, PRIVATE };
        try {
          VisibilityClient.addLabels(conf, labels);
        } catch (Throwable t) {
          throw new IOException(t);
        }
        return null;
      }
    };
    SUPERUSER.runAs(action);
  }
}
