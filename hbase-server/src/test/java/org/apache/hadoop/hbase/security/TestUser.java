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
package org.apache.hadoop.hbase.security;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

@Category({SecurityTests.class, SmallTests.class})
public class TestUser {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestUser.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestUser.class);

  @Test
  public void testCreateUserForTestingGroupCache() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    User uCreated = User.createUserForTesting(conf, "group_user", new String[] { "MYGROUP" });
    UserProvider up = UserProvider.instantiate(conf);
    User uProvided = up.create(UserGroupInformation.createRemoteUser("group_user"));
    assertArrayEquals(uCreated.getGroupNames(), uProvided.getGroupNames());

  }

  @Test
  public void testCacheGetGroups() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    UserProvider up = UserProvider.instantiate(conf);

    // VERY unlikely that this user will exist on the box.
    // This should mean the user has no groups.
    String nonUser = "kklvfnvhdhcenfnniilggljhdecjhidkle";

    // Create two UGI's for this username
    UserGroupInformation ugiOne = UserGroupInformation.createRemoteUser(nonUser);
    UserGroupInformation ugiTwo = UserGroupInformation.createRemoteUser(nonUser);

    // Now try and get the user twice.
    User uOne = up.create(ugiOne);
    User uTwo = up.create(ugiTwo);

    // Make sure that we didn't break groups and everything worked well.
    assertArrayEquals(uOne.getGroupNames(),uTwo.getGroupNames());

    // Check that they are referentially equal.
    // Since getting a group for a users that doesn't exist creates a new string array
    // the only way that they should be referentially equal is if the cache worked and
    // made sure we didn't go to hadoop's script twice.
    assertTrue(uOne.getGroupNames() == uTwo.getGroupNames());
    assertEquals(0, ugiOne.getGroupNames().length);
  }

  @Test
  public void testCacheGetGroupsRoot() throws Exception {
    // Windows users don't have a root user.
    // However pretty much every other *NIX os will have root.
    if (!SystemUtils.IS_OS_WINDOWS) {
      Configuration conf = HBaseConfiguration.create();
      UserProvider up = UserProvider.instantiate(conf);


      String rootUserName = "root";

      // Create two UGI's for this username
      UserGroupInformation ugiOne = UserGroupInformation.createRemoteUser(rootUserName);
      UserGroupInformation ugiTwo = UserGroupInformation.createRemoteUser(rootUserName);

      // Now try and get the user twice.
      User uOne = up.create(ugiOne);
      User uTwo = up.create(ugiTwo);

      // Make sure that we didn't break groups and everything worked well.
      assertArrayEquals(uOne.getGroupNames(),uTwo.getGroupNames());
      String[] groupNames = ugiOne.getGroupNames();
      assertTrue(groupNames.length > 0);
    }
  }


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
      @Override
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
      @Override
      public String run() throws Exception {
        return User.getCurrent().getName();
      }
    });
    assertEquals("User name in runAs() should match", "testuser", username);

    // verify that nested contexts work
    user2.runAs(new PrivilegedExceptionAction<Object>(){
      @Override
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
