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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Keeps lists of superusers and super groups loaded from HBase configuration,
 * checks if certain user is regarded as superuser.
 */
@InterfaceAudience.Private
public final class Superusers {
  private static final Log LOG = LogFactory.getLog(Superusers.class);

  /** Configuration key for superusers */
  public static final String SUPERUSER_CONF_KEY = "hbase.superuser"; // Not getting a name

  private static List<String> superUsers;
  private static List<String> superGroups;

  private Superusers(){}

  /**
   * Should be called only once to pre-load list of super users and super
   * groups from Configuration. This operation is idempotent.
   * @param conf configuration to load users from
   * @throws IOException if unable to initialize lists of superusers or super groups
   * @throws IllegalStateException if current user is null
   */
  public static void initialize(Configuration conf) throws IOException {
    superUsers = new ArrayList<>();
    superGroups = new ArrayList<>();
    User user = User.getCurrent();

    if (user == null) {
      throw new IllegalStateException("Unable to obtain the current user, "
        + "authorization checks for internal operations will not work correctly!");
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Current user name is " + user.getShortName());
    }
    String currentUser = user.getShortName();
    String[] superUserList = conf.getStrings(SUPERUSER_CONF_KEY, new String[0]);
    for (String name : superUserList) {
      if (AuthUtil.isGroupPrincipal(name)) {
        superGroups.add(AuthUtil.getGroupName(name));
      } else {
        superUsers.add(name);
      }
    }
    superUsers.add(currentUser);
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
        + " haven't been initialized properly.");
    }
    if (superUsers.contains(user.getShortName())) {
      return true;
    }

    for (String group : user.getGroupNames()) {
      if (superGroups.contains(group)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return true if current user is a super user (whether as user running process,
   * or declared as superuser in configuration), false otherwise.
   * @param user to check
   * @throws IllegalStateException if lists of superusers/super groups
   *   haven't been initialized properly
   * @deprecated this method is for backward compatibility, use {@link #isSuperUser(User)} instead
   */
  @Deprecated
  public static boolean isSuperUser(String user) {
    if (superUsers == null) {
      throw new IllegalStateException("Super users/super groups lists"
        + " haven't been initialized properly.");
    }
    if (superUsers.contains(user)) {
      return true;
    } else {
      return false;
    }
  }
}