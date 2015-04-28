/*
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
package org.apache.hadoop.hbase.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.ImmutableSet;

@Category({SecurityTests.class, SmallTests.class})
public class TestUser {
  private static final Log LOG = LogFactory.getLog(TestUser.class);

  @Test
  public void testBasicAttributes() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    User user = User.createUserForTesting(conf, "simple", new String[]{"foo"});
    assertEquals("Username should match", "simple", user.getName());
    assertEquals("Short username should match", "simple", user.getShortName());
    // don't test shortening of kerberos names because regular Hadoop doesn't support them
  }

  @Test
  public void testRunAs() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    final User user = User.createUserForTesting(conf, "testuser", new String[]{"foo"});
    final PrivilegedExceptionAction<String> action = new PrivilegedExceptionAction<String>(){
      public String run() throws IOException {
          User u = User.getCurrent();
          return u.getName();
      }
    };

    String username = user.runAs(action);
    assertEquals("Current user within runAs() should match",
        "testuser", username);

    // ensure the next run is correctly set
    User user2 = User.createUserForTesting(conf, "testuser2", new String[]{"foo"});
    String username2 = user2.runAs(action);
    assertEquals("Second username should match second user",
        "testuser2", username2);

    // check the exception version
    username = user.runAs(new PrivilegedExceptionAction<String>(){
      public String run() throws Exception {
        return User.getCurrent().getName();
      }
    });
    assertEquals("User name in runAs() should match", "testuser", username);

    // verify that nested contexts work
    user2.runAs(new PrivilegedExceptionAction<Object>(){
      public Object run() throws IOException, InterruptedException{
        String nestedName = user.runAs(action);
        assertEquals("Nest name should match nested user", "testuser", nestedName);
        assertEquals("Current name should match current user",
            "testuser2", User.getCurrent().getName());
        return null;
      }
    });

    username = user.runAs(new PrivilegedAction<String>(){
      String result = null;
      @Override
      public String run() {
        try {
          return User.getCurrent().getName();
        } catch (IOException e) {
          result = "empty";
        }
        return result;
      }
    });

    assertEquals("Current user within runAs() should match",
        "testuser", username);
  }

  /**
   * Make sure that we're returning a result for the current user.
   * Previously getCurrent() was returning null if not initialized on
   * non-secure Hadoop variants.
   */
  @Test
  public void testGetCurrent() throws Exception {
    User user1 = User.getCurrent();
    assertNotNull(user1.ugi);
    LOG.debug("User1 is "+user1.getName());

    for (int i =0 ; i< 100; i++) {
      User u = User.getCurrent();
      assertNotNull(u);
      assertEquals(user1.getName(), u.getName());
      assertEquals(user1, u);
      assertEquals(user1.hashCode(), u.hashCode());
    }
  }

  @Test
  public void testUserGroupNames() throws Exception {
    final String username = "testuser";
    final ImmutableSet<String> singleGroups = ImmutableSet.of("group");
    final Configuration conf = HBaseConfiguration.create();
    User user = User.createUserForTesting(conf, username,
        singleGroups.toArray(new String[singleGroups.size()]));
    assertUserGroup(user, singleGroups);

    final ImmutableSet<String> multiGroups = ImmutableSet.of("group", "group1", "group2");
    user = User.createUserForTesting(conf, username,
        multiGroups.toArray(new String[multiGroups.size()]));
    assertUserGroup(user, multiGroups);
  }

  private void assertUserGroup(User user, ImmutableSet<String> groups) {
    assertNotNull("GroupNames should be not null", user.getGroupNames());
    assertTrue("UserGroupNames length should be == " + groups.size(),
        user.getGroupNames().length == groups.size());

    for (String group : user.getGroupNames()) {
      assertTrue("groupName should be in set ", groups.contains(group));
    }
  }

  @Test
  public void testSecurityForNonSecureHadoop() {
    assertFalse("Security should be disable in non-secure Hadoop",
        User.isSecurityEnabled());

    Configuration conf = HBaseConfiguration.create();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    conf.set(User.HBASE_SECURITY_CONF_KEY, "kerberos");
    assertTrue("Security should be enabled", User.isHBaseSecurityEnabled(conf));

    conf = HBaseConfiguration.create();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    assertFalse("HBase security should not be enabled if " 
        + User.HBASE_SECURITY_CONF_KEY + " is not set accordingly",
        User.isHBaseSecurityEnabled(conf));

    conf = HBaseConfiguration.create();
    conf.set(User.HBASE_SECURITY_CONF_KEY, "kerberos");
    assertTrue("HBase security should be enabled regardless of underlying "
        + "HDFS settings", User.isHBaseSecurityEnabled(conf));
  }
}
