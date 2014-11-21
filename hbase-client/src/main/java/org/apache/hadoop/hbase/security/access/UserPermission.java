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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Represents an authorization for access over the given table, column family
 * plus qualifier, for the given user.
 */
@InterfaceAudience.Private
public class UserPermission extends TablePermission {
  private static Log LOG = LogFactory.getLog(UserPermission.class);

  private byte[] user;

  /** Nullary constructor for Writable, do not use */
  public UserPermission() {
    super();
  }

  /**
   * Creates a new instance for the given user.
   * @param user the user
   * @param assigned the list of allowed actions
   */
  public UserPermission(byte[] user, Action... assigned) {
    super(null, null, null, assigned);
    this.user = user;
  }

  /**
   * Creates a new instance for the given user,
   * matching the actions with the given codes.
   * @param user the user
   * @param actionCodes the list of allowed action codes
   */
  public UserPermission(byte[] user, byte[] actionCodes) {
    super(null, null, null, actionCodes);
    this.user = user;
  }

  /**
   * Creates a new instance for the given user.
   * @param user the user
   * @param namespace
   * @param assigned the list of allowed actions
   */
  public UserPermission(byte[] user, String namespace, Action... assigned) {
    super(namespace, assigned);
    this.user = user;
  }

  /**
   * Creates a new instance for the given user,
   * matching the actions with the given codes.
   * @param user the user
   * @param namespace
   * @param actionCodes the list of allowed action codes
   */
  public UserPermission(byte[] user, String namespace, byte[] actionCodes) {
    super(namespace, actionCodes);
    this.user = user;
  }

  /**
   * Creates a new instance for the given user, table and column family.
   * @param user the user
   * @param table the table
   * @param family the family, can be null if action is allowed over the entire
   *   table
   * @param assigned the list of allowed actions
   */
  public UserPermission(byte[] user, TableName table, byte[] family,
                        Action... assigned) {
    super(table, family, assigned);
    this.user = user;
  }

  /**
   * Creates a new permission for the given user, table, column family and
   * column qualifier.
   * @param user the user
   * @param table the table
   * @param family the family, can be null if action is allowed over the entire
   *   table
   * @param qualifier the column qualifier, can be null if action is allowed
   *   over the entire column family
   * @param assigned the list of allowed actions
   */
  public UserPermission(byte[] user, TableName table, byte[] family,
                        byte[] qualifier, Action... assigned) {
    super(table, family, qualifier, assigned);
    this.user = user;
  }

  /**
   * Creates a new instance for the given user, table, column family and
   * qualifier, matching the actions with the given codes.
   * @param user the user
   * @param table the table
   * @param family the family, can be null if action is allowed over the entire
   *   table
   * @param qualifier the column qualifier, can be null if action is allowed
   *   over the entire column family
   * @param actionCodes the list of allowed action codes
   */
  public UserPermission(byte[] user, TableName table, byte[] family,
                        byte[] qualifier, byte[] actionCodes) {
    super(table, family, qualifier, actionCodes);
    this.user = user;
  }

  /**
   * Creates a new instance for the given user, table, column family and
   * qualifier, matching the actions with the given codes.
   * @param user the user
   * @param perm a TablePermission
   */
  public UserPermission(byte[] user, TablePermission perm) {
    super(perm.getNamespace(), perm.getTableName(), perm.getFamily(), perm.getQualifier(),
        perm.actions);
    this.user = user;
  }

  public byte[] getUser() {
    return user;
  }

  /**
   * Returns true if this permission describes a global user permission.
   */
  public boolean isGlobal() {
    return(!hasTable() && !hasNamespace());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof UserPermission)) {
      return false;
    }
    UserPermission other = (UserPermission)obj;

    if ((Bytes.equals(user, other.getUser()) &&
        super.equals(obj))) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 37;
    int result = super.hashCode();
    if (user != null) {
      result = prime * result + Bytes.hashCode(user);
    }
    return result;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder("UserPermission: ")
        .append("user=").append(Bytes.toString(user))
        .append(", ").append(super.toString());
    return str.toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    user = Bytes.readByteArray(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Bytes.writeByteArray(out, user);
  }
}
