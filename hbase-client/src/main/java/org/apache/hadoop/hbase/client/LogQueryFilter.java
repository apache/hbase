/*
 *
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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Slow/Large Log Query Filter with all filter and limit parameters
 * Extends generic LogRequest used by Admin API getLogEntries
 * @deprecated as of 2.4.0. Will be removed in 4.0.0.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@Deprecated
public class LogQueryFilter {

  private String regionName;
  private String clientAddress;
  private String tableName;
  private String userName;
  private int limit = 10;
  private Type type = Type.SLOW_LOG;
  private FilterByOperator filterByOperator = FilterByOperator.OR;

  public enum Type {
    SLOW_LOG,
    LARGE_LOG
  }

  public enum FilterByOperator {
    AND,
    OR
  }

  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public String getClientAddress() {
    return clientAddress;
  }

  public void setClientAddress(String clientAddress) {
    this.clientAddress = clientAddress;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public FilterByOperator getFilterByOperator() {
    return filterByOperator;
  }

  public void setFilterByOperator(FilterByOperator filterByOperator) {
    this.filterByOperator = filterByOperator;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LogQueryFilter that = (LogQueryFilter) o;

    return new EqualsBuilder()
      .append(limit, that.limit)
      .append(regionName, that.regionName)
      .append(clientAddress, that.clientAddress)
      .append(tableName, that.tableName)
      .append(userName, that.userName)
      .append(type, that.type)
      .append(filterByOperator, that.filterByOperator)
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(regionName)
      .append(clientAddress)
      .append(tableName)
      .append(userName)
      .append(limit)
      .append(type)
      .append(filterByOperator)
      .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("regionName", regionName)
      .append("clientAddress", clientAddress)
      .append("tableName", tableName)
      .append("userName", userName)
      .append("limit", limit)
      .append("type", type)
      .append("filterByOperator", filterByOperator)
      .toString();
  }

}
