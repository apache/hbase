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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

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
 *  <p>
 * Internally, in this class, we cache the instances to limit the number of objects and
 *  make the "equals" faster. We try to minimize the number of objects created of
 *  the number of array copy to check if we already have an instance of this TableName. The code
 *  is not optimize for a new instance creation but is optimized to check for existence.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class TableName implements Comparable<TableName> {

  /** See {@link #createTableNameIfNecessary(ByteBuffer, ByteBuffer)} */
  private static final Set<TableName> tableCache = new CopyOnWriteArraySet<TableName>();

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

  private final byte[] name;
  private final String nameAsString;
  private final byte[] namespace;
  private final String namespaceAsString;
  private final byte[] qualifier;
  private final String qualifierAsString;
  private final boolean systemTable;
  private final int hashCode;

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
    if (namespaceDelimIndex < 0){
      isLegalTableQualifierName(tableName);
    } else {
      isLegalNamespaceName(tableName, 0, namespaceDelimIndex);
      isLegalTableQualifierName(tableName, namespaceDelimIndex + 1, tableName.length);
    }
    return tableName;
  }

  public static byte [] isLegalTableQualifierName(final byte[] qualifierName) {
    isLegalTableQualifierName(qualifierName, 0, qualifierName.length, false);
    return qualifierName;
  }

  public static byte [] isLegalTableQualifierName(final byte[] qualifierName, boolean isSnapshot) {
    isLegalTableQualifierName(qualifierName, 0, qualifierName.length, isSnapshot);
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
                                                int end) {
      isLegalTableQualifierName(qualifierName, start, end, false);
  }

  public static void isLegalTableQualifierName(final byte[] qualifierName,
                                                int start,
                                                int end,
                                                boolean isSnapshot) {
    if(end - start < 1) {
      throw new IllegalArgumentException(isSnapshot ? "Snapshot" : "Table" + " qualifier must not be empty");
    }

    if (qualifierName[start] == '.' || qualifierName[start] == '-') {
      throw new IllegalArgumentException("Illegal first character <" + qualifierName[start] +
                                         "> at 0. " + (isSnapshot ? "Snapshot" : "User-space table") +
                                         " qualifiers can only start with 'alphanumeric " +
                                         "characters': i.e. [a-zA-Z_0-9]: " +
                                         Bytes.toString(qualifierName, start, end));
    }
    for (int i = start; i < end; i++) {
      if (Character.isLetterOrDigit(qualifierName[i]) ||
          qualifierName[i] == '_' ||
          qualifierName[i] == '-' ||
          qualifierName[i] == '.') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character code:" + qualifierName[i] +
                                         ", <" + (char) qualifierName[i] + "> at " + i +
                                         ". " + (isSnapshot ? "Snapshot" : "User-space table") +
                                         " qualifiers can only contain " +
                                         "'alphanumeric characters': i.e. [a-zA-Z_0-9-.]: " +
                                         Bytes.toString(qualifierName, start, end));
    }
  }
  public static void isLegalNamespaceName(byte[] namespaceName) {
    isLegalNamespaceName(namespaceName, 0, namespaceName.length);
  }

  /**
   * Valid namespace characters are [a-zA-Z_0-9]
   */
  public static void isLegalNamespaceName(final byte[] namespaceName,
                                           final int start,
                                           final int end) {
    if(end - start < 1) {
      throw new IllegalArgumentException("Namespace name must not be empty");
    }
    for (int i = start; i < end; i++) {
      if (Character.isLetterOrDigit(namespaceName[i])|| namespaceName[i] == '_') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + namespaceName[i] +
        "> at " + i + ". Namespaces can only contain " +
        "'alphanumeric characters': i.e. [a-zA-Z_0-9]: " + Bytes.toString(namespaceName,
          start, end));
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

  /**
   * Ideally, getNameAsString should contain namespace within it,
   * but if the namespace is default, it just returns the name. This method
   * takes care of this corner case.
   */
  public String getNameWithNamespaceInclAsString() {
    if(getNamespaceAsString().equals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR)) {
      return NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR +
          TableName.NAMESPACE_DELIM + getNameAsString();
    }
    return getNameAsString();
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

  /**
   *
   * @throws IllegalArgumentException See {@link #valueOf(byte[])}
   */
  private TableName(ByteBuffer namespace, ByteBuffer qualifier) throws IllegalArgumentException {
    this.qualifier = new byte[qualifier.remaining()];
    qualifier.duplicate().get(this.qualifier);
    this.qualifierAsString = Bytes.toString(this.qualifier);

    if (qualifierAsString.equals(OLD_ROOT_STR)) {
      throw new IllegalArgumentException(OLD_ROOT_STR + " has been deprecated.");
    }
    if (qualifierAsString.equals(OLD_META_STR)) {
      throw new IllegalArgumentException(OLD_META_STR + " no longer exists. The table has been " +
          "renamed to " + META_TABLE_NAME);
    }

    if (Bytes.equals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME, namespace)) {
      // Using the same objects: this will make the comparison faster later
      this.namespace = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME;
      this.namespaceAsString = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;
      this.systemTable = false;

      // The name does not include the namespace when it's the default one.
      this.nameAsString = qualifierAsString;
      this.name = this.qualifier;
    } else {
      if (Bytes.equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME, namespace)) {
        this.namespace = NamespaceDescriptor.SYSTEM_NAMESPACE_NAME;
        this.namespaceAsString = NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR;
        this.systemTable = true;
      } else {
        this.namespace = new byte[namespace.remaining()];
        namespace.duplicate().get(this.namespace);
        this.namespaceAsString = Bytes.toString(this.namespace);
        this.systemTable = false;
      }
      this.nameAsString = namespaceAsString + NAMESPACE_DELIM + qualifierAsString;
      this.name = Bytes.toBytes(nameAsString);
    }

    this.hashCode = nameAsString.hashCode();

    isLegalNamespaceName(this.namespace);
    isLegalTableQualifierName(this.qualifier);
  }

  /**
   * This is only for the old and meta tables.
   */
  private TableName(String qualifier) {
    this.qualifier = Bytes.toBytes(qualifier);
    this.qualifierAsString = qualifier;

    this.namespace = NamespaceDescriptor.SYSTEM_NAMESPACE_NAME;
    this.namespaceAsString = NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR;
    this.systemTable = true;

    // WARNING: nameAsString is different than name for old meta & root!
    // This is by design.
    this.nameAsString = namespaceAsString + NAMESPACE_DELIM + qualifierAsString;
    this.name = this.qualifier;

    this.hashCode = nameAsString.hashCode();
  }


  /**
   * Check that the object does not exist already. There are two reasons for creating the objects
   * only once:
   * 1) With 100K regions, the table names take ~20MB.
   * 2) Equals becomes much faster as it's resolved with a reference and an int comparison.
   */
  private static TableName createTableNameIfNecessary(ByteBuffer bns, ByteBuffer qns) {
    for (TableName tn : tableCache) {
      if (Bytes.equals(tn.getQualifier(), qns) && Bytes.equals(tn.getNamespace(), bns)) {
        return tn;
      }
    }

    TableName newTable = new TableName(bns, qns);
    if (tableCache.add(newTable)) {  // Adds the specified element if it is not already present
      return newTable;
    }

    // Someone else added it. Let's find it.
    for (TableName tn : tableCache) {
      if (Bytes.equals(tn.getQualifier(), qns) && Bytes.equals(tn.getNamespace(), bns)) {
        return tn;
      }
    }
    // this should never happen.
    throw new IllegalStateException(newTable + " was supposed to be in the cache");
  }


  /**
   * It is used to create table names for old META, and ROOT table.
   * These tables are not really legal tables. They are not added into the cache.
   * @return a dummy TableName instance (with no validation) for the passed qualifier
   */
  private static TableName getADummyTableName(String qualifier) {
    return new TableName(qualifier);
  }


  public static TableName valueOf(String namespaceAsString, String qualifierAsString) {
    if (namespaceAsString == null || namespaceAsString.length() < 1) {
      namespaceAsString = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;
    }

    for (TableName tn : tableCache) {
      if (qualifierAsString.equals(tn.getQualifierAsString()) &&
          namespaceAsString.equals(tn.getNameAsString())) {
        return tn;
      }
    }

    return createTableNameIfNecessary(
        ByteBuffer.wrap(Bytes.toBytes(namespaceAsString)),
        ByteBuffer.wrap(Bytes.toBytes(qualifierAsString)));
  }


  /**
   * @throws IllegalArgumentException if fullName equals old root or old meta. Some code
   *  depends on this. The test is buried in the table creation to save on array comparison
   *  when we're creating a standard table object that will be in the cache.
   */
  public static TableName valueOf(byte[] fullName) throws IllegalArgumentException{
    for (TableName tn : tableCache) {
      if (Arrays.equals(tn.getName(), fullName)) {
        return tn;
      }
    }

    int namespaceDelimIndex = com.google.common.primitives.Bytes.lastIndexOf(fullName,
        (byte) NAMESPACE_DELIM);

    if (namespaceDelimIndex < 0) {
      return createTableNameIfNecessary(
          ByteBuffer.wrap(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME),
          ByteBuffer.wrap(fullName));
    } else {
      return createTableNameIfNecessary(
          ByteBuffer.wrap(fullName, 0, namespaceDelimIndex),
          ByteBuffer.wrap(fullName, namespaceDelimIndex + 1,
              fullName.length - (namespaceDelimIndex + 1)));
    }
  }


  /**
   * @throws IllegalArgumentException if fullName equals old root or old meta. Some code
   *  depends on this.
   */
  public static TableName valueOf(String name) {
    for (TableName tn : tableCache) {
      if (name.equals(tn.getNameAsString())) {
        return tn;
      }
    }

    int namespaceDelimIndex = name.indexOf(NAMESPACE_DELIM);
    byte[] nameB = Bytes.toBytes(name);

    if (namespaceDelimIndex < 0) {
      return createTableNameIfNecessary(
          ByteBuffer.wrap(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME),
          ByteBuffer.wrap(nameB));
    } else {
      return createTableNameIfNecessary(
          ByteBuffer.wrap(nameB, 0, namespaceDelimIndex),
          ByteBuffer.wrap(nameB, namespaceDelimIndex + 1,
              nameB.length - (namespaceDelimIndex + 1)));
    }
  }


  public static TableName valueOf(byte[] namespace, byte[] qualifier) {
    if (namespace == null || namespace.length < 1) {
      namespace = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME;
    }

    for (TableName tn : tableCache) {
      if (Arrays.equals(tn.getQualifier(), qualifier) &&
          Arrays.equals(tn.getNamespace(), namespace)) {
        return tn;
      }
    }

    return createTableNameIfNecessary(
        ByteBuffer.wrap(namespace), ByteBuffer.wrap(qualifier));
  }

  public static TableName valueOf(ByteBuffer namespace, ByteBuffer qualifier) {
    if (namespace == null || namespace.remaining() < 1) {
      return createTableNameIfNecessary(
          ByteBuffer.wrap(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME), qualifier);
    }

    return createTableNameIfNecessary(namespace, qualifier);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TableName tableName = (TableName) o;

    return o.hashCode() == hashCode && nameAsString.equals(tableName.nameAsString);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  /**
   * For performance reasons, the ordering is not lexicographic.
   */
  @Override
  public int compareTo(TableName tableName) {
    if (this == tableName) return 0;
    if (this.hashCode < tableName.hashCode()) {
      return -1;
    }
    if (this.hashCode > tableName.hashCode()) {
      return 1;
    }
    return this.nameAsString.compareTo(tableName.getNameAsString());
  }

  /**
   * Get the appropriate row comparator for this table.
   *
   * @return The comparator.
   * @deprecated The comparator is an internal property of the table. Should
   * not have been exposed here
   */
  @InterfaceAudience.Private
  @Deprecated
  public KVComparator getRowComparator() {
     if(TableName.META_TABLE_NAME.equals(this)) {
      return KeyValue.META_COMPARATOR;
    }
    return KeyValue.COMPARATOR;
  }
}
