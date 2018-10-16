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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;

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
  private AuthResult.Params params;

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
    this.params = new Params().setTableName(table).setFamily(family).setQualifier(qualifier);
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
    this.params = new Params().setTableName(table).setFamilies(families);
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
    this.params = new Params().setNamespace(namespace);
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

  public Params getParams() { return this.params;}

  public void setAllowed(boolean allowed) {
    this.allowed = allowed;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  private static String toFamiliesString(Map<byte[], ? extends Collection<?>> families,
      byte[] family, byte[] qual) {
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
      if (qual != null) {
        sb.append(":").append(Bytes.toString(qual));
      }
    }
    return sb.toString();
  }

  public String toContextString() {
    StringBuilder sb = new StringBuilder();
    String familiesString = toFamiliesString(families, family, qualifier);
    sb.append("(user=")
        .append(user != null ? user.getName() : "UNKNOWN")
        .append(", ");
    sb.append("scope=")
        .append(namespace != null ? namespace :
            table == null ? "GLOBAL" : table.getNameWithNamespaceInclAsString())
        .append(", ");
    if(namespace == null && familiesString.length() > 0) {
      sb.append("family=")
        .append(familiesString)
        .append(", ");
    }
    String paramsString = params.toString();
    if(paramsString.length() > 0) {
      sb.append("params=[")
          .append(paramsString)
          .append("],");
    }
    sb.append("action=")
        .append(action != null ? action.toString() : "")
        .append(")");
    return sb.toString();
  }

  @Override
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

  public String toFamilyString() {
    return toFamiliesString(families, family, qualifier);
  }

  public static class Params {
    private String namespace = null;
    private TableName tableName = null;
    private Map<byte[], ? extends Collection<?>> families = null;
    byte[] family = null;
    byte[] qualifier = null;
    // For extra parameters to be shown in audit log
    private final Map<String, String> extraParams = new HashMap<String, String>(2);

    public Params addExtraParam(String key, String value) {
      extraParams.put(key, value);
      return this;
    }

    public Params setNamespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Params setTableName(TableName table) {
      this.tableName = table;
      return this;
    }

    public Params setFamilies(Map<byte[], ? extends Collection<?>> families) {
      this.families = families;
      return this;
    }

    public Params setFamily(byte[] family) {
      this.family = family;
      return this;
    }

    public Params setQualifier(byte[] qualifier) {
      this.qualifier = qualifier;
      return this;
    }

    @Override
    public String toString() {
      String familiesString = toFamiliesString(families, family, qualifier);
      String[] params = new String[] {
          namespace != null ? "namespace=" + namespace : null,
          tableName != null ? "table=" + tableName.getNameWithNamespaceInclAsString() : null,
          familiesString.length() > 0 ? "family=" + familiesString : null,
          extraParams.isEmpty() ? null : concatenateExtraParams()
      };
      return Joiner.on(",").skipNulls().join(params);
    }

    /**
     * @return extra parameter key/value string
     */
    private String concatenateExtraParams() {
      final StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Entry<String, String> entry : extraParams.entrySet()) {
        if (entry.getKey() != null && entry.getValue() != null) {
          if (!first) {
            sb.append(',');
          }
          first = false;
          sb.append(entry.getKey() + '=');
          sb.append(entry.getValue());
        }
      }
      return sb.toString();
    }
  }
}
