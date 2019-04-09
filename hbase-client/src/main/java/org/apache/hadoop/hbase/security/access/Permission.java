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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.VersionedWritable;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Base permissions instance representing the ability to perform a given set
 * of actions.
 *
 * @see TablePermission
 */
@InterfaceAudience.Public
public class Permission extends VersionedWritable {
  protected static final byte VERSION = 0;

  @InterfaceAudience.Public
  public enum Action {
    READ('R'), WRITE('W'), EXEC('X'), CREATE('C'), ADMIN('A');

    private final byte code;
    Action(char code) {
      this.code = (byte) code;
    }

    public byte code() { return code; }
  }

  @InterfaceAudience.Private
  protected enum Scope {
    GLOBAL('G'), NAMESPACE('N'), TABLE('T'), EMPTY('E');

    private final byte code;
    Scope(char code) {
      this.code = (byte) code;
    }

    public byte code() {
      return code;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(Permission.class);

  protected static final Map<Byte, Action> ACTION_BY_CODE;
  protected static final Map<Byte, Scope> SCOPE_BY_CODE;

  protected EnumSet<Action> actions = EnumSet.noneOf(Action.class);
  protected Scope scope = Scope.EMPTY;

  static {
    ACTION_BY_CODE = ImmutableMap.of(
      Action.READ.code, Action.READ,
      Action.WRITE.code, Action.WRITE,
      Action.EXEC.code, Action.EXEC,
      Action.CREATE.code, Action.CREATE,
      Action.ADMIN.code, Action.ADMIN
    );

    SCOPE_BY_CODE = ImmutableMap.of(
      Scope.GLOBAL.code, Scope.GLOBAL,
      Scope.NAMESPACE.code, Scope.NAMESPACE,
      Scope.TABLE.code, Scope.TABLE,
      Scope.EMPTY.code, Scope.EMPTY
    );
  }

  /** Empty constructor for Writable implementation.  <b>Do not use.</b> */
  public Permission() {
    super();
  }

  public Permission(Action... assigned) {
    if (assigned != null && assigned.length > 0) {
      actions.addAll(Arrays.asList(assigned));
    }
  }

  public Permission(byte[] actionCodes) {
    if (actionCodes != null) {
      for (byte code : actionCodes) {
        Action action = ACTION_BY_CODE.get(code);
        if (action == null) {
          LOG.error("Ignoring unknown action code '" +
            Bytes.toStringBinary(new byte[] { code }) + "'");
          continue;
        }
        actions.add(action);
      }
    }
  }

  public Action[] getActions() {
    return actions.toArray(new Action[actions.size()]);
  }

  /**
   * check if given action is granted
   * @param action action to be checked
   * @return true if granted, false otherwise
   */
  public boolean implies(Action action) {
    return actions.contains(action);
  }

  public void setActions(Action[] assigned) {
    if (assigned != null && assigned.length > 0) {
      // setActions should cover the previous actions,
      // so we call clear here.
      actions.clear();
      actions.addAll(Arrays.asList(assigned));
    }
  }

  /**
   * Check if two permission equals regardless of actions. It is useful when
   * merging a new permission with an existed permission which needs to check two permissions's
   * fields.
   * @param obj instance
   * @return true if equals, false otherwise
   */
  public boolean equalsExceptActions(Object obj) {
    return obj instanceof Permission;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Permission)) {
      return false;
    }

    Permission other = (Permission) obj;
    if (actions.isEmpty() && other.actions.isEmpty()) {
      return true;
    } else if (!actions.isEmpty() && !other.actions.isEmpty()) {
      if (actions.size() != other.actions.size()) {
        return false;
      }
      return actions.containsAll(other.actions);
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 37;
    int result = 23;
    for (Action a : actions) {
      result = prime * result + a.code();
    }
    result = prime * result + scope.code();
    return result;
  }

  @Override
  public String toString() {
    return "[Permission: " + rawExpression() + "]";
  }

  protected String rawExpression() {
    StringBuilder raw = new StringBuilder("actions=");
    if (actions != null) {
      int i = 0;
      for (Action action : actions) {
        if (i > 0) {
          raw.append(",");
        }
        raw.append(action != null ? action.toString() : "NULL");
        i++;
      }
    }
    return raw.toString();
  }

  /** @return the object version number */
  @Override
  public byte getVersion() {
    return VERSION;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int length = (int) in.readByte();
    actions = EnumSet.noneOf(Action.class);
    if (length > 0) {
      for (int i = 0; i < length; i++) {
        byte b = in.readByte();
        Action action = ACTION_BY_CODE.get(b);
        if (action == null) {
          throw new IOException("Unknown action code '" +
            Bytes.toStringBinary(new byte[] { b }) + "' in input");
        }
        actions.add(action);
      }
    }
    scope = SCOPE_BY_CODE.get(in.readByte());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeByte(actions != null ? actions.size() : 0);
    if (actions != null) {
      for (Action a: actions) {
        out.writeByte(a.code());
      }
    }
    out.writeByte(scope.code());
  }

  public Scope getAccessScope() {
    return scope;
  }

  /**
   * Build a global permission
   * @return global permission builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Build a namespace permission
   * @param namespace the specific namespace
   * @return namespace permission builder
   */
  public static Builder newBuilder(String namespace) {
    return new Builder(namespace);
  }

  /**
   * Build a table permission
   * @param tableName the specific table name
   * @return table permission builder
   */
  public static Builder newBuilder(TableName tableName) {
    return new Builder(tableName);
  }

  public static final class Builder {
    private String namespace;
    private TableName tableName;
    private byte[] family;
    private byte[] qualifier;
    private List<Action> actions = new ArrayList<>();

    private Builder() {
    }

    private Builder(String namespace) {
      this.namespace = namespace;
    }

    private Builder(TableName tableName) {
      this.tableName = tableName;
    }

    public Builder withFamily(byte[] family) {
      Objects.requireNonNull(tableName, "The tableName can't be NULL");
      this.family = family;
      return this;
    }

    public Builder withQualifier(byte[] qualifier) {
      Objects.requireNonNull(tableName, "The tableName can't be NULL");
      this.qualifier = qualifier;
      return this;
    }

    public Builder withActions(Action... actions) {
      for (Action action : actions) {
        if (action != null) {
          this.actions.add(action);
        }
      }
      return this;
    }

    public Builder withActionCodes(byte[] actionCodes) {
      if (actionCodes != null) {
        for (byte code : actionCodes) {
          Action action = ACTION_BY_CODE.get(code);
          if (action == null) {
            LOG.error("Ignoring unknown action code '{}'",
              Bytes.toStringBinary(new byte[] { code }));
            continue;
          }
          this.actions.add(action);
        }
      }
      return this;
    }

    public Permission build() {
      Action[] actionArray = actions.toArray(new Action[actions.size()]);
      if (namespace != null) {
        return new NamespacePermission(namespace, actionArray);
      } else if (tableName != null) {
        return new TablePermission(tableName, family, qualifier, actionArray);
      } else {
        return new GlobalPermission(actionArray);
      }
    }
  }

}
