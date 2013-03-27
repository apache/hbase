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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Represents the result of an authorization check for logging and error
 * reporting.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AuthResult {
  private final boolean allowed;
  private final byte[] table;
  private final byte[] family;
  private final byte[] qualifier;
  private final Permission.Action action;
  private final String request;
  private final String reason;
  private final User user;

  public AuthResult(boolean allowed, String request, String reason,  User user,
      Permission.Action action, byte[] table, byte[] family, byte[] qualifier) {
    this.allowed = allowed;
    this.request = request;
    this.reason = reason;
    this.user = user;
    this.table = table;
    this.family = family;
    this.qualifier = qualifier;
    this.action = action;
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

  public byte[] getTable() {
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

  public String toContextString() {
    return "(user=" + (user != null ? user.getName() : "UNKNOWN") + ", " +
        "scope=" + (table == null ? "GLOBAL" : Bytes.toString(table)) + ", " +
        "family=" + (family != null ? Bytes.toString(family) : "") + ", " +
        "qualifer=" + (qualifier != null ? Bytes.toString(qualifier) : "") + ", " +
        "action=" + (action != null ? action.toString() : "") + ")";
  }

  public String toString() {
    return "AuthResult" + toContextString();
  }

  public static AuthResult allow(String request, String reason, User user, Permission.Action action,
      byte[] table, byte[] family, byte[] qualifier) {
    return new AuthResult(true, request, reason, user, action, table, family, qualifier);
  }

  public static AuthResult allow(String request, String reason, User user, Permission.Action action, byte[] table) {
    return new AuthResult(true, request, reason, user, action, table, null, null);
  }

  public static AuthResult deny(String request, String reason, User user,
      Permission.Action action, byte[] table) {
    return new AuthResult(false, request, reason, user, action, table, null, null);
  }

  public static AuthResult deny(String request, String reason, User user,
      Permission.Action action, byte[] table, byte[] family, byte[] qualifier) {
    return new AuthResult(false, request, reason, user, action, table, family, qualifier);
  }
}