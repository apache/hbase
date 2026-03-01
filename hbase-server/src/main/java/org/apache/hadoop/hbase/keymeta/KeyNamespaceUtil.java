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
package org.apache.hadoop.hbase.keymeta;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Utility class for constructing key namespaces used in key management operations.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class KeyNamespaceUtil {
  private KeyNamespaceUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  /**
   * Construct a key namespace from a table descriptor and column family descriptor.
   * @param tableDescriptor The table descriptor
   * @param family          The column family descriptor
   * @return The constructed key namespace
   */
  public static String constructKeyNamespace(TableDescriptor tableDescriptor,
    ColumnFamilyDescriptor family) {
    return tableDescriptor.getTableName().getNameAsString() + "/" + family.getNameAsString();
  }

  /**
   * Construct a key namespace from a store context.
   * @param storeContext The store context
   * @return The constructed key namespace
   */
  public static String constructKeyNamespace(StoreContext storeContext) {
    return storeContext.getTableName().getNameAsString() + "/"
      + storeContext.getFamily().getNameAsString();
  }

  /**
   * Construct a key namespace by deriving table name and family name from a store file info.
   * @param fileInfo The store file info
   * @return The constructed key namespace
   */
  public static String constructKeyNamespace(StoreFileInfo fileInfo) {
    return constructKeyNamespace(
      fileInfo.isLink() ? fileInfo.getLink().getOriginPath() : fileInfo.getPath());
  }

  /**
   * Construct a key namespace by deriving table name and family name from a store file path.
   * @param path The path
   * @return The constructed key namespace
   */
  public static String constructKeyNamespace(Path path) {
    return constructKeyNamespace(path.getParent().getParent().getParent().getName(),
      path.getParent().getName());
  }

  /**
   * Construct a key namespace from a table name and family name.
   * @param tableName The table name
   * @param family    The family name
   * @return The constructed key namespace
   */
  public static String constructKeyNamespace(String tableName, String family) {
    // Add precoditions for null check
    Preconditions.checkNotNull(tableName, "tableName should not be null");
    Preconditions.checkNotNull(family, "family should not be null");
    return tableName + "/" + family;
  }
}
