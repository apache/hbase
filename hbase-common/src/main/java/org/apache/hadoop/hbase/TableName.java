/**
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

package org.apache.hadoop.hbase;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Immutable POJO class for representing a table name.
 * Which is of the form:
 * &lt;table namespace&gt;:&lt;table qualifier&gt;
 *
 * Two special namespaces:
 *
 * 1. hbase - system namespace, used to contain hbase internal tables
 * 2. default - tables with no explicit specified namespace will
 * automatically fall into this namespace.
 *
 * ie
 *
 * a) foo:bar, means namespace=foo and qualifier=bar
 * b) bar, means namespace=default and qualifier=bar
 * c) default:bar, means namespace=default and qualifier=bar
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class TableName implements Comparable<TableName> {

  /** Namespace delimiter */
  //this should always be only 1 byte long
  public final static char NAMESPACE_DELIM = ':';

  // A non-capture group so that this can be embedded.
  // regex is a bit more complicated to support nuance of tables
  // in default namespace
  //Allows only letters, digits and '_'
  public static final String VALID_NAMESPACE_REGEX =
      "(?:[a-zA-Z_0-9]+)";
  //Allows only letters, digits, '_', '-' and '.'
  public static final String VALID_TABLE_QUALIFIER_REGEX =
      "(?:[a-zA-Z_0-9][a-zA-Z_0-9-.]*)";
  //Concatenation of NAMESPACE_REGEX and TABLE_QUALIFIER_REGEX,
  //with NAMESPACE_DELIM as delimiter
  public static final String VALID_USER_TABLE_REGEX =
      "(?:(?:(?:"+VALID_NAMESPACE_REGEX+"\\"+NAMESPACE_DELIM+")?)" +
         "(?:"+VALID_TABLE_QUALIFIER_REGEX+"))";

  /** The hbase:meta table's name. */
  public static final TableName META_TABLE_NAME =
      valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "meta");

  /** The Namespace table's name. */
  public static final TableName NAMESPACE_TABLE_NAME =
      valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "namespace");

  public static final String OLD_META_STR = ".META.";
  public static final String OLD_ROOT_STR = "-ROOT-";

  /**
   * TableName for old -ROOT- table. It is used to read/process old WALs which have
   * ROOT edits.
   */
  public static final TableName OLD_ROOT_TABLE_NAME = getADummyTableName(OLD_ROOT_STR);
  /**
   * TableName for old .META. table. Used in testing.
   */
  public static final TableName OLD_META_TABLE_NAME = getADummyTableName(OLD_META_STR);

  private byte[] name;
  private String nameAsString;
  private byte[] namespace;
  private String namespaceAsString;
  private byte[] qualifier;
  private String qualifierAsString;
  private boolean systemTable;

  private TableName() {}

  /**
   * Check passed byte array, "tableName", is legal user-space table name.
   * @return Returns passed <code>tableName</code> param
   * @throws IllegalArgumentException if passed a tableName is null or
   * is made of other than 'word' characters or underscores: i.e.
   * <code>[a-zA-Z_0-9.-:]</code>. The ':' is used to delimit the namespace
   * from the table name and can be used for nothing else.
   *
   * Namespace names can only contain 'word' characters
   * <code>[a-zA-Z_0-9]</code> or '_'
   *
   * Qualifier names can only contain 'word' characters
   * <code>[a-zA-Z_0-9]</code> or '_', '.' or '-'.
   * The name may not start with '.' or '-'.
   *
   * Valid fully qualified table names:
   * foo:bar, namespace=>foo, table=>bar
   * org:foo.bar, namespace=org, table=>foo.bar
   */
  public static byte [] isLegalFullyQualifiedTableName(final byte[] tableName) {
    if (tableName == null || tableName.length <= 0) {
      throw new IllegalArgumentException("Name is null or empty");
    }
    int namespaceDelimIndex = com.google.common.primitives.Bytes.lastIndexOf(tableName,
        (byte) NAMESPACE_DELIM);
    if (namespaceDelimIndex == 0 || namespaceDelimIndex == -1){
      isLegalTableQualifierName(tableName);
    } else {
      isLegalNamespaceName(tableName, 0, namespaceDelimIndex);
      isLegalTableQualifierName(tableName, namespaceDelimIndex + 1, tableName.length);
    }
    return tableName;
  }

  public static byte [] isLegalTableQualifierName(final byte[] qualifierName){
    isLegalTableQualifierName(qualifierName, 0, qualifierName.length);
    return qualifierName;
  }

  /**
   * Qualifier names can only contain 'word' characters
   * <code>[a-zA-Z_0-9]</code> or '_', '.' or '-'.
   * The name may not start with '.' or '-'.
   *
   * @param qualifierName byte array containing the qualifier name
   * @param start start index
   * @param end end index (exclusive)
   */
  public static void isLegalTableQualifierName(final byte[] qualifierName,
                                                int start,
                                                int end){
    if(end - start < 1) {
      throw new IllegalArgumentException("Table qualifier must not be empty");
    }
    if (qualifierName[start] == '.' || qualifierName[start] == '-') {
      throw new IllegalArgumentException("Illegal first character <" + qualifierName[0] +
          "> at 0. Namespaces can only start with alphanumeric " +
          "characters': i.e. [a-zA-Z_0-9]: " + Bytes.toString(qualifierName));
    }
    for (int i = start; i < end; i++) {
      if (Character.isLetterOrDigit(qualifierName[i]) ||
          qualifierName[i] == '_' ||
          qualifierName[i] == '-' ||
          qualifierName[i] == '.') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + qualifierName[i] +
        "> at " + i + ". User-space table qualifiers can only contain " +
        "'alphanumeric characters': i.e. [a-zA-Z_0-9-.]: " +
          Bytes.toString(qualifierName, start, end));
    }
  }

  public static void isLegalNamespaceName(byte[] namespaceName) {
    isLegalNamespaceName(namespaceName, 0, namespaceName.length);
  }

  /**
   * Valid namespace characters are [a-zA-Z_0-9]
   * @param namespaceName
   * @param offset
   * @param length
   */
  public static void isLegalNamespaceName(byte[] namespaceName, int offset, int length) {
    for (int i = offset; i < length; i++) {
      if (Character.isLetterOrDigit(namespaceName[i])|| namespaceName[i] == '_') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + namespaceName[i] +
        "> at " + i + ". Namespaces can only contain " +
        "'alphanumeric characters': i.e. [a-zA-Z_0-9]: " + Bytes.toString(namespaceName,
          offset, length));
    }
  }

  public byte[] getName() {
    return name;
  }

  public String getNameAsString() {
    return nameAsString;
  }

  public byte[] getNamespace() {
    return namespace;
  }

  public String getNamespaceAsString() {
    return namespaceAsString;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public String getQualifierAsString() {
    return qualifierAsString;
  }

  public byte[] toBytes() {
    return name;
  }

  public boolean isSystemTable() {
    return systemTable;
  }

  @Override
  public String toString() {
    return nameAsString;
  }

  public static TableName valueOf(byte[] namespace, byte[] qualifier) {
    TableName ret = new TableName();
    if(namespace == null || namespace.length < 1) {
      namespace = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME;
    }
    ret.namespace = namespace;
    ret.namespaceAsString = Bytes.toString(namespace);
    ret.qualifier = qualifier;
    ret.qualifierAsString = Bytes.toString(qualifier);

    finishValueOf(ret);

    return ret;
  }

  /**
   * It is used to create table names for old META, and ROOT table.
   * @return a dummy TableName instance (with no validation) for the passed qualifier
   */
  private static TableName getADummyTableName(String qualifier) {
    TableName ret = new TableName();
    ret.namespaceAsString = NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR;
    ret.qualifierAsString = qualifier;
    ret.nameAsString = createFullyQualified(ret.namespaceAsString, ret.qualifierAsString);
    ret.name = Bytes.toBytes(qualifier);
    return ret;
  }
  public static TableName valueOf(String namespaceAsString, String qualifierAsString) {
    TableName ret = new TableName();
    if(namespaceAsString == null || namespaceAsString.length() < 1) {
      namespaceAsString = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;
    }
    ret.namespaceAsString = namespaceAsString;
    ret.namespace = Bytes.toBytes(namespaceAsString);
    ret.qualifier = Bytes.toBytes(qualifierAsString);
    ret.qualifierAsString = qualifierAsString;

    finishValueOf(ret);

    return ret;
  }

  private static void finishValueOf(TableName tableName) {
    isLegalNamespaceName(tableName.namespace);
    isLegalTableQualifierName(tableName.qualifier);

    tableName.nameAsString =
        createFullyQualified(tableName.namespaceAsString, tableName.qualifierAsString);
    tableName.name = Bytes.toBytes(tableName.nameAsString);
    tableName.systemTable = Bytes.equals(
      tableName.namespace, NamespaceDescriptor.SYSTEM_NAMESPACE_NAME);
  }

  public static TableName valueOf(byte[] name) {
    return valueOf(Bytes.toString(name));
  }

  public static TableName valueOf(String name) {
    if(name.equals(OLD_ROOT_STR)) {
      throw new IllegalArgumentException(OLD_ROOT_STR + " has been deprecated.");
    }
    if(name.equals(OLD_META_STR)) {
      throw new IllegalArgumentException(OLD_META_STR + " no longer exists. The table has been " +
          "renamed to "+META_TABLE_NAME);
    }

    isLegalFullyQualifiedTableName(Bytes.toBytes(name));
    int index = name.indexOf(NAMESPACE_DELIM);
    if (index != -1) {
      return TableName.valueOf(name.substring(0, index), name.substring(index + 1));
    }
    return TableName.valueOf(NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), name);
  }

  private static String createFullyQualified(String namespace, String tableQualifier) {
    if (namespace.equals(NamespaceDescriptor.DEFAULT_NAMESPACE.getName())) {
      return tableQualifier;
    }
    return namespace+NAMESPACE_DELIM+tableQualifier;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TableName tableName = (TableName) o;

    if (!nameAsString.equals(tableName.nameAsString)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = nameAsString.hashCode();
    return result;
  }

  @Override
  public int compareTo(TableName tableName) {
    return this.nameAsString.compareTo(tableName.getNameAsString());
  }

  /**
   * Get the appropriate row comparator for this table.
   *
   * @return The comparator.
   */
  public KVComparator getRowComparator() {
     if(TableName.META_TABLE_NAME.equals(this)) {
      return KeyValue.META_COMPARATOR;
    }
    return KeyValue.COMPARATOR;
  }
}
