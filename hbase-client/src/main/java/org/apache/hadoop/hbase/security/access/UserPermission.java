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

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * UserPermission consists of a user name and a permission.
 * Permission can be one of [Global, Namespace, Table] permission.
 */
@InterfaceAudience.Private
public class UserPermission {

  private String user;
  private Permission permission;

  /**
   * Construct a global user permission.
   * @param user user name
   * @param assigned assigned actions
   */
  public UserPermission(String user, Permission.Action... assigned) {
    this.user = user;
    this.permission = new GlobalPermission(assigned);
  }

  /**
   * Construct a global user permission.
   * @param user user name
   * @param actionCode action codes
   */
  public UserPermission(String user, byte[] actionCode) {
    this.user = user;
    this.permission = new GlobalPermission(actionCode);
  }

  /**
   * Construct a namespace user permission.
   * @param user user name
   * @param namespace namespace
   * @param assigned assigned actions
   */
  public UserPermission(String user, String namespace, Permission.Action... assigned) {
    this.user = user;
    this.permission = new NamespacePermission(namespace, assigned);
  }

  /**
   * Construct a table user permission.
   * @param user user name
   * @param tableName table name
   * @param assigned assigned actions
   */
  public UserPermission(String user, TableName tableName, Permission.Action... assigned) {
    this.user = user;
    this.permission = new TablePermission(tableName, assigned);
  }

  /**
   * Construct a table:family user permission.
   * @param user user name
   * @param tableName table name
   * @param family family name of table
   * @param assigned assigned actions
   */
  public UserPermission(String user, TableName tableName, byte[] family,
    Permission.Action... assigned) {
    this(user, tableName, family, null, assigned);
  }

  /**
   * Construct a table:family:qualifier user permission.
   * @param user user name
   * @param tableName table name
   * @param family family name of table
   * @param qualifier qualifier name of table
   * @param assigned assigned actions
   */
  public UserPermission(String user, TableName tableName, byte[] family, byte[] qualifier,
      Permission.Action... assigned) {
    this.user = user;
    this.permission = new TablePermission(tableName, family, qualifier, assigned);
  }

  /**
   * Construct a table:family:qualifier user permission.
   * @param user user name
   * @param tableName table name
   * @param family family name of table
   * @param qualifier qualifier name of table
   * @param actionCodes assigned actions
   */
  public UserPermission(String user, TableName tableName, byte[] family, byte[] qualifier,
      byte[] actionCodes) {
    this.user = user;
    this.permission = new TablePermission(tableName, family, qualifier, actionCodes);
  }

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
