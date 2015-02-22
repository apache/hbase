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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Get, remove and modify table descriptors.
 * Used by servers to host descriptors.
 */
@InterfaceAudience.Private
public interface TableDescriptors {
  /**
   * @param tableName
   * @return HTableDescriptor for tablename
   * @throws IOException
   */
  HTableDescriptor get(final TableName tableName)
  throws IOException;

  /**
   * @param tableName
   * @return TableDescriptor for tablename
   * @throws IOException
   */
  TableDescriptor getDescriptor(final TableName tableName)
      throws IOException;

  /**
   * Get Map of all NamespaceDescriptors for a given namespace.
   * @return Map of all descriptors.
   * @throws IOException
   */
  Map<String, HTableDescriptor> getByNamespace(String name)
  throws IOException;

  /**
   * Get Map of all HTableDescriptors. Populates the descriptor cache as a
   * side effect.
   * @return Map of all descriptors.
   * @throws IOException
   */
  Map<String, HTableDescriptor> getAll()
  throws IOException;

  /**
   * Get Map of all TableDescriptors. Populates the descriptor cache as a
   * side effect.
   * @return Map of all descriptors.
   * @throws IOException
   */
  Map<String, TableDescriptor> getAllDescriptors()
      throws IOException;

  /**
   * Add or update descriptor
   * @param htd Descriptor to set into TableDescriptors
   * @throws IOException
   */
  void add(final HTableDescriptor htd)
  throws IOException;

  /**
   * Add or update descriptor
   * @param htd Descriptor to set into TableDescriptors
   * @throws IOException
   */
  void add(final TableDescriptor htd)
      throws IOException;

  /**
   * @param tablename
   * @return Instance of table descriptor or null if none found.
   * @throws IOException
   */
  HTableDescriptor remove(final TableName tablename)
  throws IOException;

  /**
   * Enables the tabledescriptor cache
   */
  void setCacheOn() throws IOException;

  /**
   * Disables the tabledescriptor cache
   */
  void setCacheOff() throws IOException;
}
