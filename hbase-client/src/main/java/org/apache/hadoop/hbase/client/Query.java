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
package org.apache.hadoop.hbase.client;

import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.security.access.AccessControlConstants;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Query extends OperationWithAttributes {
  private static final String ISOLATION_LEVEL = "_isolationlevel_";
  protected Filter filter = null;

  /**
   * @return Filter
   */
  public Filter getFilter() {
    return filter;
  }

  /**
   * Apply the specified server-side filter when performing the Query.
   * Only {@link Filter#filterKeyValue(Cell)} is called AFTER all tests
   * for ttl, column match, deletes and max versions have been run.
   * @param filter filter to run on the server
   * @return this for invocation chaining
   */
  public Query setFilter(Filter filter) {
    this.filter = filter;
    return this;
  }

  /**
   * Sets the authorizations to be used by this Query
   * @param authorizations
   */
  public void setAuthorizations(Authorizations authorizations) {
    this.setAttribute(VisibilityConstants.VISIBILITY_LABELS_ATTR_KEY, ProtobufUtil
        .toAuthorizations(authorizations).toByteArray());
  }

  /**
   * @return The authorizations this Query is associated with.
   * @throws DeserializationException
   */
  public Authorizations getAuthorizations() throws DeserializationException {
    byte[] authorizationsBytes = this.getAttribute(VisibilityConstants.VISIBILITY_LABELS_ATTR_KEY);
    if (authorizationsBytes == null) return null;
    return ProtobufUtil.toAuthorizations(authorizationsBytes);
  }

  /**
   * @return The serialized ACL for this operation, or null if none
   */
  public byte[] getACL() {
    return getAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL);
  }

  /**
   * @param user User short name
   * @param perms Permissions for the user
   */
  public void setACL(String user, Permission perms) {
    setAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL,
      ProtobufUtil.toUsersAndPermissions(user, perms).toByteArray());
  }

  /**
   * @param perms A map of permissions for a user or users
   */
  public void setACL(Map<String, Permission> perms) {
    ListMultimap<String, Permission> permMap = ArrayListMultimap.create();
    for (Map.Entry<String, Permission> entry : perms.entrySet()) {
      permMap.put(entry.getKey(), entry.getValue());
    }
    setAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL,
      ProtobufUtil.toUsersAndPermissions(permMap).toByteArray());
  }

  /**
   * @deprecated No effect
   */
  @Deprecated
  public boolean getACLStrategy() {
    return false;
  }

  /**
   * @deprecated No effect
   */
  @Deprecated
  public void setACLStrategy(boolean cellFirstStrategy) {
  }

  /**
   * Set the isolation level for this query. If the
   * isolation level is set to READ_UNCOMMITTED, then
   * this query will return data from committed and
   * uncommitted transactions. If the isolation level
   * is set to READ_COMMITTED, then this query will return
   * data from committed transactions only. If a isolation
   * level is not explicitly set on a Query, then it
   * is assumed to be READ_COMMITTED.
   * @param level IsolationLevel for this query
   */
  public void setIsolationLevel(IsolationLevel level) {
    setAttribute(ISOLATION_LEVEL, level.toBytes());
  }

  /**
   * @return The isolation level of this scan.
   * If no isolation level was set for this scan object,
   * then it returns READ_COMMITTED.
   * @return The IsolationLevel for this scan
   */
  public IsolationLevel getIsolationLevel() {
    byte[] attr = getAttribute(ISOLATION_LEVEL);
    return attr == null ? IsolationLevel.READ_COMMITTED :
                          IsolationLevel.fromBytes(attr);
  }
}
