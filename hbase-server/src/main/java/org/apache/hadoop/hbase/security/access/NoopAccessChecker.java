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

import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * NoopAccessChecker is returned when hbase.security.authorization is not enabled.
 * Always allow authorization if any user require any permission.
 */
@InterfaceAudience.Private
public final class NoopAccessChecker extends AccessChecker {

  public NoopAccessChecker(Configuration conf) throws RuntimeException {
    super(conf);
  }

  @Override
  public void requireAccess(User user, String request, TableName tableName, Action... permissions) {
  }

  @Override
  public void requirePermission(User user, String request, String filterUser, Action perm) {
    requireGlobalPermission(user, request, perm, null, null, filterUser);
  }

  @Override
  public void requireGlobalPermission(User user, String request, Action perm, TableName tableName,
      Map<byte[], ? extends Collection<byte[]>> familyMap, String filterUser) {
  }

  @Override
  public void requireGlobalPermission(User user, String request, Action perm, String namespace) {
  }

  @Override
  public void requireNamespacePermission(User user, String request, String namespace,
      String filterUser, Action... permissions) {
  }

  @Override
  public void requireNamespacePermission(User user, String request, String namespace,
      TableName tableName, Map<byte[], ? extends Collection<byte[]>> familyMap,
      Action... permissions) {
  }

  @Override
  public void requirePermission(User user, String request, TableName tableName, byte[] family,
      byte[] qualifier, String filterUser, Action... permissions) {
  }

  @Override
  public void requireTablePermission(User user, String request, TableName tableName, byte[] family,
      byte[] qualifier, Action... permissions) {
  }

  @Override
  public void performOnSuperuser(String request, User caller, String userToBeChecked) {
  }

  @Override
  public void checkLockPermissions(User user, String namespace, TableName tableName,
      RegionInfo[] regionInfos, String reason) {
  }

  @Override
  public boolean hasUserPermission(User user, String request, Permission permission) {
    return true;
  }

  @Override
  public AuthResult permissionGranted(String request, User user, Action permRequest,
      TableName tableName, Map<byte[], ? extends Collection<?>> families) {
    return AuthResult.allow(request, "All users allowed because authorization is disabled", user,
      permRequest, tableName, families);
  }
}
