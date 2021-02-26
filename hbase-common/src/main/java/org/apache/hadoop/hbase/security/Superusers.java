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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps lists of superusers and super groups loaded from HBase configuration,
 * checks if certain user is regarded as superuser.
 */
@InterfaceAudience.Private
public final class Superusers {
  private static final Logger LOG = LoggerFactory.getLogger(Superusers.class);

  /** Configuration key for superusers */
  public static final String SUPERUSER_CONF_KEY = "hbase.superuser"; // Not getting a name

  private static Set<String> superUsers;
  private static Set<String> superGroups;
  private static User systemUser;

  private Superusers(){}

  /**
   * Should be called only once to pre-load list of super users and super
   * groups from Configuration. This operation is idempotent.
   * @param conf configuration to load users from
   * @throws IOException if unable to initialize lists of superusers or super groups
   * @throws IllegalStateException if current user is null
   */
  public static void initialize(Configuration conf) throws IOException {
    superUsers = new HashSet<>();
    superGroups = new HashSet<>();
    systemUser = User.getCurrent();

    if (systemUser == null) {
      throw new IllegalStateException("Unable to obtain the current user, "
        + "authorization checks for internal operations will not work correctly!");
    }

    String currentUser = systemUser.getShortName();
    LOG.trace("Current user name is {}", currentUser);
    superUsers.add(currentUser);

    String[] superUserList = conf.getStrings(SUPERUSER_CONF_KEY, new String[0]);
    for (String name : superUserList) {
      if (AuthUtil.isGroupPrincipal(name)) {
        // Let's keep the '@' for distinguishing from user.
        superGroups.add(name);
      } else {
        superUsers.add(name);
      }
    }
  }

  /**
   * @return true if current user is a super user (whether as user running process,
   * declared as individual superuser or member of supergroup), false otherwise.
   * @param user to check
   * @throws IllegalStateException if lists of superusers/super groups
   *   haven't been initialized properly
   */
  public static boolean isSuperUser(User user) {
    if (superUsers == null) {
      throw new IllegalStateException("Super users/super groups lists"
        + " have not been initialized properly.");
    }
    if (user == null){
      throw new IllegalArgumentException("Null user passed for super user check");
    }
    if (superUsers.contains(user.getShortName())) {
      return true;
    }
    for (String group : user.getGroupNames()) {
      if (superGroups.contains(AuthUtil.toGroupEntry(group))) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return true if current user is a super user, false otherwise.
   * @param user to check
   */
  public static boolean isSuperUser(String user) {
    return superUsers.contains(user) || superGroups.contains(user);
  }

  public static Collection<String> getSuperUsers() {
    return superUsers;
  }

  public static Collection<String> getSuperGroups() {
    return superGroups;
  }

  public static User getSystemUser() {
    return systemUser;
  }
}
