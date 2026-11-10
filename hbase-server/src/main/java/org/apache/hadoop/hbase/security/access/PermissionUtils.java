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
package org.apache.hadoop.hbase.security.access;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class PermissionUtils {

  private static Set<String> getUserGroups(MutableObject<Set<String>> userGroupsHolder,
    String username) {
    Set<String> userGroups = userGroupsHolder.get();
    if (userGroups != null) {
      return userGroups;
    }
    userGroups = new HashSet<String>(AccessChecker.getUserGroups(username));
    userGroupsHolder.setValue(userGroups);
    return userGroups;
  }

  /**
   * Filter user permissions based on username. Just delegate call to
   * {@link #filter(List, byte[], byte[], String)} with null {@code cf} and {@code cq}.
   */
  public static List<UserPermission> filter(List<UserPermission> userPerms, String username) {
    return filter(userPerms, null, null, username);
  }

  /**
   * Filter user permissions based on column family, column qualifier, and username.
   * <p>
   * Notice that, if {@code cf} or {@code cq} is not null, we will assume that the permission is a
   * table permission.
   * <p>
   * When there is group permission, i.e, the user field for the {@link UserPermission} is start
   * with {@literal @}, then we will check whether the given {@code username} belongs to this group.
   */
  public static List<UserPermission> filter(List<UserPermission> userPerms, byte[] cf, byte[] cq,
    String username) {
    if (cf == null && cq == null && username == null) {
      // nothing to filter
      return userPerms;
    }
    Stream<UserPermission> stream = userPerms.stream();
    if (cf != null) {
      stream =
        stream.filter(up -> Bytes.equals(((TablePermission) up.getPermission()).getFamily(), cf));
    }
    if (cq != null) {
      stream = stream
        .filter(up -> Bytes.equals(((TablePermission) up.getPermission()).getQualifier(), cq));
    }
    if (username != null) {
      MutableObject<Set<String>> userGroupsHolder = new MutableObject<>();
      stream = stream.filter(up -> {
        String u = up.getUser();
        if (u.charAt(0) == '@') {
          // this is a group, check whether the given user belongs to this group
          return getUserGroups(userGroupsHolder, username).contains(u.substring(1));
        }
        return u.equals(username);
      });
    }
    return stream.collect(Collectors.toList());
  }
}
