/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.access;

import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Represents the result of an authorization check for logging and error
 * reporting.
 */
@InterfaceAudience.Private
public class AuthResult {
  private boolean allowed;
  private final String namespace;
  private final TableName table;
  private final Permission.Action action;
  private final String request;
  private String reason;
  private final User user;

  // "family" and "qualifier" should only be used if "families" is null.
  private final byte[] family;
  private final byte[] qualifier;
  private final Map<byte[], ? extends Collection<?>> families;

  public AuthResult(boolean allowed, String request, String reason, User user,
      Permission.Action action, TableName table, byte[] family, byte[] qualifier) {
    this.allowed = allowed;
    this.request = request;
    this.reason = reason;
    this.user = user;
    this.table = table;
    this.family = family;
    this.qualifier = qualifier;
    this.action = action;
    this.families = null;
    this.namespace = null;
  }

  public AuthResult(boolean allowed, String request, String reason, User user,
        Permission.Action action, TableName table,
        Map<byte[], ? extends Collection<?>> families) {
    this.allowed = allowed;
    this.request = request;
    this.reason = reason;
    this.user = user;
    this.table = table;
    this.family = null;
    this.qualifier = null;
    this.action = action;
    this.families = families;
    this.namespace = null;
  }

  public AuthResult(boolean allowed, String request, String reason, User user,
        Permission.Action action, String namespace) {
    this.allowed = allowed;
    this.request = request;
    this.reason = reason;
    this.user = user;
    this.namespace = namespace;
    this.action = action;
    this.table = null;
    this.family = null;
    this.qualifier = null;
    this.families = null;
  }

  public boolean isAllowed() {
    return allowed;
  }

  public User getUser() {
    return user;
  }

  public String getReason() {
    return reason;
  }

  public TableName getTableName() {
    return table;
  }

  public byte[] getFamily() {
    return family;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public Permission.Action getAction() {
    return action;
  }

  public String getRequest() {
    return request;
  }

  public void setAllowed(boolean allowed) {
    this.allowed = allowed;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  String toFamilyString() {
    StringBuilder sb = new StringBuilder();
    if (families != null) {
      boolean first = true;
      for (Map.Entry<byte[], ? extends Collection<?>> entry : families.entrySet()) {
        String familyName = Bytes.toString(entry.getKey());
        if (entry.getValue() != null && !entry.getValue().isEmpty()) {
          for (Object o : entry.getValue()) {
            String qualifier;
            if (o instanceof byte[]) {
              qualifier = Bytes.toString((byte[])o);
            } else if (o instanceof Cell) {
              Cell c = (Cell) o;
              qualifier = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(),
                  c.getQualifierLength());
            } else {
              // Shouldn't really reach this?
              qualifier = o.toString();
            }
            if (!first) {
              sb.append("|");
            }
            first = false;
            sb.append(familyName).append(":").append(qualifier);
          }
        } else {
          if (!first) {
            sb.append("|");
          }
          first = false;
          sb.append(familyName);
        }
      }
    } else if (family != null) {
      sb.append(Bytes.toString(family));
      if (qualifier != null) {
        sb.append(":").append(Bytes.toString(qualifier));
      }
    }
    return sb.toString();
  }

  public String toContextString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(user=")
        .append(user != null ? user.getName() : "UNKNOWN")
        .append(", ");
    sb.append("scope=")
        .append(namespace != null ? namespace : table == null ? "GLOBAL" : table);
    if(namespace == null) {
      sb.append(", ")
        .append("family=")
        .append(toFamilyString())
        .append(", ");
    }
    sb.append("action=")
        .append(action != null ? action.toString() : "")
        .append(")");
    return sb.toString();
  }

  public String toString() {
    return "AuthResult" + toContextString();
  }

  public static AuthResult allow(String request, String reason, User user,
      Permission.Action action, String namespace) {
    return new AuthResult(true, request, reason, user, action, namespace);
  }

  public static AuthResult allow(String request, String reason, User user,
      Permission.Action action, TableName table, byte[] family, byte[] qualifier) {
    return new AuthResult(true, request, reason, user, action, table, family, qualifier);
  }

  public static AuthResult allow(String request, String reason, User user,
      Permission.Action action, TableName table,
      Map<byte[], ? extends Collection<?>> families) {
    return new AuthResult(true, request, reason, user, action, table, families);
  }

  public static AuthResult deny(String request, String reason, User user,
      Permission.Action action, String namespace) {
    return new AuthResult(false, request, reason, user, action, namespace);
  }

  public static AuthResult deny(String request, String reason, User user,
      Permission.Action action, TableName table, byte[] family, byte[] qualifier) {
    return new AuthResult(false, request, reason, user, action, table, family, qualifier);
  }

  public static AuthResult deny(String request, String reason, User user,
        Permission.Action action, TableName table,
        Map<byte[], ? extends Collection<?>> families) {
    return new AuthResult(false, request, reason, user, action, table, families);
  }
}
