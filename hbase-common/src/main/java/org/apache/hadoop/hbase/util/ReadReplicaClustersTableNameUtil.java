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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;

public class ReadReplicaClustersTableNameUtil {

  /**
   * Utility method to determine if TableName is a meta TableName without taking hbase.meta.table.suffix into account.
   * @param tableName TableName to determine if isMeta
   * @return if this TableName contains the default meta table name
   */
  public static boolean isMetaTableNameWithoutSuffix(TableName tableName) {
    String[] parts = tableName.getNameWithNamespaceInclAsString().split(String.valueOf(TableName.NAMESPACE_DELIM));
    return parts[0].equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR) &&
        parts[1].startsWith(TableName.DEFAULT_META_TABLE_NAME_STR);
  }
}
