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

import java.util.Objects;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * UserPermission consists of a user name and a permission.
 * Permission can be one of [Global, Namespace, Table] permission.
 */
@InterfaceAudience.Public
public class UserPermission {

  private String user;
  private Permission permission;

  /**
   * Construct a user permission given permission.
   * @param user user name
   * @param permission one of [Global, Namespace, Table] permission
   */
  public UserPermission(String user, Permission permission) {
    this.user = user;
    this.permission = permission;
  }

  /**
   * Get this permission access scope.
   * @return access scope
   */
  public Permission.Scope getAccessScope() {
    return permission.getAccessScope();
  }

  public String getUser() {
    return user;
  }

  public Permission getPermission() {
    return permission;
  }

  public boolean equalsExceptActions(Object obj) {
    if (!(obj instanceof UserPermission)) {
      return false;
    }
    UserPermission other = (UserPermission) obj;
    return user.equals(other.user) && permission.equalsExceptActions(other.permission);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof UserPermission)) {
      return false;
    }
    UserPermission other = (UserPermission) obj;
    return user.equals(other.user) && permission.equals(other.permission);
  }

  @Override
  public int hashCode() {
    final int prime = 37;
    int result = permission.hashCode();
    if (user != null) {
      result = prime * result + Objects.hashCode(user);
    }
    return result;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder("UserPermission: ")
        .append("user=").append(user)
        .append(", ").append(permission.toString());
    return str.toString();
  }
}
