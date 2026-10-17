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

import java.util.Set;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Read-only view of the in-memory ACL state: the only part of the ACL storage {@link AuthManager}
 * depends on. Implementations are owned by the process side that maintains the state
 * ({@code ACLManager} on the master, {@code AclStore} on a region server).
 */
@InterfaceAudience.Private
public interface AclView {

  /**
   * Global-scope permission of the given user or group entry name.
   * @return the permission, or {@code null} when the entry has no global grant
   */
  GlobalPermission getGlobalPermission(String entryName);

  /**
   * Namespace-scope permissions of the given user or group entry name.
   * @return the permissions, empty when the entry has no namespace grant
   */
  Set<NamespacePermission> getNamespacePermissions(String namespace, String entryName);

  /**
   * Table-scope permissions of the given user or group entry name.
   * @return the permissions, empty when the entry has no table grant
   */
  Set<TablePermission> getTablePermissions(TableName table, String entryName);
}
