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
package org.apache.hadoop.hbase.client;

import java.util.Map;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.security.access.AccessControlConstants;
import org.apache.hadoop.hbase.security.access.AccessControlUtil;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Base class for HBase read operations; e.g. Scan and Get.
 */
@InterfaceAudience.Public
public abstract class Query extends OperationWithAttributes {
  private static final String ISOLATION_LEVEL = "_isolationlevel_";
  protected Filter filter = null;
  protected int targetReplicaId = -1;
  protected Consistency consistency = Consistency.STRONG;
  protected Map<byte[], TimeRange> colFamTimeRangeMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
  protected Boolean loadColumnFamiliesOnDemand = null;

  /**
   * n
   */
  public Filter getFilter() {
    return filter;
  }

  /**
   * Apply the specified server-side filter when performing the Query. Only
   * {@link Filter#filterCell(org.apache.hadoop.hbase.Cell)} is called AFTER all tests for ttl,
   * column match, deletes and column family's max versions have been run.
   * @param filter filter to run on the server
   * @return this for invocation chaining
   */
  public Query setFilter(Filter filter) {
    this.filter = filter;
    return this;
  }

  /**
   * Sets the authorizations to be used by this Query n
   */
  public Query setAuthorizations(Authorizations authorizations) {
    this.setAttribute(VisibilityConstants.VISIBILITY_LABELS_ATTR_KEY,
      ProtobufUtil.toAuthorizations(authorizations).toByteArray());
    return this;
  }

  /**
   * @return The authorizations this Query is associated with. n
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
   * @param user  User short name
   * @param perms Permissions for the user
   */
  public Query setACL(String user, Permission perms) {
    setAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL,
      AccessControlUtil.toUsersAndPermissions(user, perms).toByteArray());
    return this;
  }

  /**
   * @param perms A map of permissions for a user or users
   */
  public Query setACL(Map<String, Permission> perms) {
    ListMultimap<String, Permission> permMap = ArrayListMultimap.create();
    for (Map.Entry<String, Permission> entry : perms.entrySet()) {
      permMap.put(entry.getKey(), entry.getValue());
    }
    setAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL,
      AccessControlUtil.toUsersAndPermissions(permMap).toByteArray());
    return this;
  }

  /**
   * Returns the consistency level for this operation
   * @return the consistency level
   */
  public Consistency getConsistency() {
    return consistency;
  }

  /**
   * Sets the consistency level for this operation
   * @param consistency the consistency level
   */
  public Query setConsistency(Consistency consistency) {
    this.consistency = consistency;
    return this;
  }

  /**
   * Specify region replica id where Query will fetch data from. Use this together with
   * {@link #setConsistency(Consistency)} passing {@link Consistency#TIMELINE} to read data from a
   * specific replicaId. <br>
   * <b> Expert: </b>This is an advanced API exposed. Only use it if you know what you are doing n
   */
  public Query setReplicaId(int Id) {
    this.targetReplicaId = Id;
    return this;
  }

  /**
   * Returns region replica id where Query will fetch data from.
   * @return region replica id or -1 if not set.
   */
  public int getReplicaId() {
    return this.targetReplicaId;
  }

  /**
   * Set the isolation level for this query. If the isolation level is set to READ_UNCOMMITTED, then
   * this query will return data from committed and uncommitted transactions. If the isolation level
   * is set to READ_COMMITTED, then this query will return data from committed transactions only. If
   * a isolation level is not explicitly set on a Query, then it is assumed to be READ_COMMITTED.
   * @param level IsolationLevel for this query
   */
  public Query setIsolationLevel(IsolationLevel level) {
    setAttribute(ISOLATION_LEVEL, level.toBytes());
    return this;
  }

  /**
   * @return The isolation level of this query. If no isolation level was set for this query object,
   *         then it returns READ_COMMITTED.
   * @return The IsolationLevel for this query
   */
  public IsolationLevel getIsolationLevel() {
    byte[] attr = getAttribute(ISOLATION_LEVEL);
    return attr == null ? IsolationLevel.READ_COMMITTED : IsolationLevel.fromBytes(attr);
  }

  /**
   * Set the value indicating whether loading CFs on demand should be allowed (cluster default is
   * false). On-demand CF loading doesn't load column families until necessary, e.g. if you filter
   * on one column, the other column family data will be loaded only for the rows that are included
   * in result, not all rows like in normal case. With column-specific filters, like
   * SingleColumnValueFilter w/filterIfMissing == true, this can deliver huge perf gains when
   * there's a cf with lots of data; however, it can also lead to some inconsistent results, as
   * follows: - if someone does a concurrent update to both column families in question you may get
   * a row that never existed, e.g. for { rowKey = 5, { cat_videos =&gt; 1 }, { video =&gt; "my cat"
   * } } someone puts rowKey 5 with { cat_videos =&gt; 0 }, { video =&gt; "my dog" }, concurrent
   * scan filtering on "cat_videos == 1" can get { rowKey = 5, { cat_videos =&gt; 1 }, { video =&gt;
   * "my dog" } }. - if there's a concurrent split and you have more than 2 column families, some
   * rows may be missing some column families.
   */
  public Query setLoadColumnFamiliesOnDemand(boolean value) {
    this.loadColumnFamiliesOnDemand = value;
    return this;
  }

  /**
   * Get the raw loadColumnFamiliesOnDemand setting; if it's not set, can be null.
   */
  public Boolean getLoadColumnFamiliesOnDemandValue() {
    return this.loadColumnFamiliesOnDemand;
  }

  /**
   * Get the logical value indicating whether on-demand CF loading should be allowed.
   */
  public boolean doLoadColumnFamiliesOnDemand() {
    return (this.loadColumnFamiliesOnDemand != null) && this.loadColumnFamiliesOnDemand;
  }

  /**
   * Get versions of columns only within the specified timestamp range, [minStamp, maxStamp) on a
   * per CF bases. Note, default maximum versions to return is 1. If your time range spans more than
   * one version and you want all versions returned, up the number of versions beyond the default.
   * Column Family time ranges take precedence over the global time range.
   * @param cf       the column family for which you want to restrict
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive n
   */

  public Query setColumnFamilyTimeRange(byte[] cf, long minStamp, long maxStamp) {
    colFamTimeRangeMap.put(cf, new TimeRange(minStamp, maxStamp));
    return this;
  }

  /**
   * @return A map of column families to time ranges
   */
  public Map<byte[], TimeRange> getColumnFamilyTimeRange() {
    return this.colFamTimeRangeMap;
  }
}
