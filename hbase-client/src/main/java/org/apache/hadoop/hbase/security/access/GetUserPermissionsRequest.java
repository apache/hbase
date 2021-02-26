/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.access;

import java.util.Objects;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used by
 * {@link org.apache.hadoop.hbase.client.Admin#getUserPermissions(GetUserPermissionsRequest)}.
 * Represents the params of user permissions needed to get from HBase.
 */
@InterfaceAudience.Public
public final class GetUserPermissionsRequest {
  private String userName;
  private String namespace;
  private TableName tableName;
  private byte[] family;
  private byte[] qualifier;

  private GetUserPermissionsRequest(String userName, String namespace, TableName tableName,
      byte[] family, byte[] qualifier) {
    this.userName = userName;
    this.namespace = namespace;
    this.tableName = tableName;
    this.family = family;
    this.qualifier = qualifier;
  }

  /**
   * Build a get global permission request
   * @return a get global permission request builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Build a get namespace permission request
   * @param namespace the specific namespace
   * @return a get namespace permission request builder
   */
  public static Builder newBuilder(String namespace) {
    return new Builder(namespace);
  }

  /**
   * Build a get table permission request
   * @param tableName the specific table name
   * @return a get table permission request builder
   */
  public static Builder newBuilder(TableName tableName) {
    return new Builder(tableName);
  }

  public String getUserName() {
    return userName;
  }

  public String getNamespace() {
    return namespace;
  }

  public TableName getTableName() {
    return tableName;
  }

  public byte[] getFamily() {
    return family;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public static final class Builder {
    private String userName;
    private String namespace;
    private TableName tableName;
    private byte[] family;
    private byte[] qualifier;

    private Builder() {
    }

    private Builder(String namespace) {
      this.namespace = namespace;
    }

    private Builder(TableName tableName) {
      this.tableName = tableName;
    }

    /**
     * user name could be null if need all global/namespace/table permissions
     */
    public Builder withUserName(String userName) {
      this.userName = userName;
      return this;
    }

    public Builder withFamily(byte[] family) {
      Objects.requireNonNull(tableName, "The tableName can't be NULL");
      this.family = family;
      return this;
    }

    public Builder withQualifier(byte[] qualifier) {
      Objects.requireNonNull(tableName, "The tableName can't be NULL");
      // Objects.requireNonNull(family, "The family can't be NULL");
      this.qualifier = qualifier;
      return this;
    }

    public GetUserPermissionsRequest build() {
      return new GetUserPermissionsRequest(userName, namespace, tableName, family, qualifier);
    }
  }
}
