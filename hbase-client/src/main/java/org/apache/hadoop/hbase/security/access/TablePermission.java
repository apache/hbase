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

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Represents an authorization for access for the given actions, optionally
 * restricted to the given column family or column qualifier, over the
 * given table. If the family property is <code>null</code>, it implies
 * full table access.
 */
@InterfaceAudience.Public
public class TablePermission extends Permission {

  private TableName table;
  private byte[] family;
  private byte[] qualifier;

  /**
   * Construct a table:family:qualifier permission.
   * @param table table name
   * @param family family name
   * @param qualifier qualifier name
   * @param assigned assigned actions
   */
  TablePermission(TableName table, byte[] family, byte[] qualifier, Action... assigned) {
    super(assigned);
    this.table = table;
    this.family = family;
    this.qualifier = qualifier;
    this.scope = Scope.TABLE;
  }

  public TableName getTableName() {
    return table;
  }

  public boolean hasFamily() {
    return family != null;
  }

  public byte[] getFamily() {
    return family;
  }

  public boolean hasQualifier() {
    return qualifier != null;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public String getNamespace() {
    return table.getNamespaceAsString();
  }

  /**
   * Check if given action can performs on given table:family:qualifier.
   * @param table table name
   * @param family family name
   * @param qualifier qualifier name
   * @param action one of [Read, Write, Create, Exec, Admin]
   * @return true if can, false otherwise
   */
  public boolean implies(TableName table, byte[] family, byte[] qualifier, Action action) {
    if (failCheckTable(table)) {
      return false;
    }
    if (failCheckFamily(family)) {
      return false;
    }
    if (failCheckQualifier(qualifier)) {
      return false;
    }
    return implies(action);
  }

  /**
   * Check if given action can performs on given table:family.
   * @param table table name
   * @param family family name
   * @param action one of [Read, Write, Create, Exec, Admin]
   * @return true if can, false otherwise
   */
  public boolean implies(TableName table, byte[] family, Action action) {
    if (failCheckTable(table)) {
      return false;
    }
    if (failCheckFamily(family)) {
      return false;
    }
    return implies(action);
  }

  private boolean failCheckTable(TableName table) {
    return this.table == null || !this.table.equals(table);
  }

  private boolean failCheckFamily(byte[] family) {
    return this.family != null && (family == null || !Bytes.equals(this.family, family));
  }

  private boolean failCheckQualifier(byte[] qual) {
    return this.qualifier != null && (qual == null || !Bytes.equals(this.qualifier, qual));
  }

  /**
   * Checks if this permission grants access to perform the given action on
   * the given table and key value.
   * @param table the table on which the operation is being performed
   * @param kv the KeyValue on which the operation is being requested
   * @param action the action requested
   * @return <code>true</code> if the action is allowed over the given scope
   *   by this permission, otherwise <code>false</code>
   */
  public boolean implies(TableName table, KeyValue kv, Action action) {
    if (failCheckTable(table)) {
      return false;
    }

    if (family != null && !(CellUtil.matchingFamily(kv, family))) {
      return false;
    }

    if (qualifier != null && !(CellUtil.matchingQualifier(kv, qualifier))) {
      return false;
    }

    // check actions
    return super.implies(action);
  }

  /**
   * Check if fields of table in table permission equals.
   * @param tp to be checked table permission
   * @return true if equals, false otherwise
   */
  public boolean tableFieldsEqual(TablePermission tp) {
    if (tp == null) {
      return false;
    }

    boolean tEq = (table == null && tp.table == null) || (table != null && table.equals(tp.table));
    boolean fEq = (family == null && tp.family == null) || Bytes.equals(family, tp.family);
    boolean qEq = (qualifier == null && tp.qualifier == null) ||
                   Bytes.equals(qualifier, tp.qualifier);
    return tEq && fEq && qEq;
  }

  @Override
  public boolean equalsExceptActions(Object obj) {
    if (!(obj instanceof TablePermission)) {
      return false;
    }
    TablePermission other = (TablePermission) obj;
    return tableFieldsEqual(other);
  }

  @Override
  public boolean equals(Object obj) {
    return equalsExceptActions(obj) && super.equals(obj);
  }

  @Override
  public int hashCode() {
    final int prime = 37;
    int result = super.hashCode();
    if (table != null) {
      result = prime * result + table.hashCode();
    }
    if (family != null) {
      result = prime * result + Bytes.hashCode(family);
    }
    if (qualifier != null) {
      result = prime * result + Bytes.hashCode(qualifier);
    }
    return result;
  }

  @Override
  public String toString() {
    return "[TablePermission: " + rawExpression() + "]";
  }

  @Override
  protected String rawExpression() {
    StringBuilder raw = new StringBuilder();
    if (table != null) {
      raw.append("table=").append(table)
         .append(", family=").append(family == null ? null : Bytes.toString(family))
         .append(", qualifier=").append(qualifier == null ? null : Bytes.toString(qualifier))
         .append(", ");
    }
    return raw.toString() + super.rawExpression();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    byte[] tableBytes = Bytes.readByteArray(in);
    if(tableBytes.length > 0) {
      table = TableName.valueOf(tableBytes);
    }
    if (in.readBoolean()) {
      family = Bytes.readByteArray(in);
    }
    if (in.readBoolean()) {
      qualifier = Bytes.readByteArray(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    // Explicitly writing null to maintain se/deserialize backward compatibility.
    Bytes.writeByteArray(out, table == null ? null : table.getName());
    out.writeBoolean(family != null);
    if (family != null) {
      Bytes.writeByteArray(out, family);
    }
    out.writeBoolean(qualifier != null);
    if (qualifier != null) {
      Bytes.writeByteArray(out, qualifier);
    }
  }
}
