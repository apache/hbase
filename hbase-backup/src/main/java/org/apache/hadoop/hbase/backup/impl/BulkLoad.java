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
package org.apache.hadoop.hbase.backup.impl;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The data corresponding to a single bulk-loaded file that is being tracked by the backup logic.
 */
@InterfaceAudience.Private
public class BulkLoad {
  private final TableName tableName;
  private final String region;
  private final String columnFamily;
  private final String hfilePath;
  private final byte[] rowKey;

  public BulkLoad(TableName tableName, String region, String columnFamily, String hfilePath,
    byte[] rowKey) {
    this.tableName = tableName;
    this.region = region;
    this.columnFamily = columnFamily;
    this.hfilePath = hfilePath;
    this.rowKey = rowKey;
  }

  public TableName getTableName() {
    return tableName;
  }

  public String getRegion() {
    return region;
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public String getHfilePath() {
    return hfilePath;
  }

  public byte[] getRowKey() {
    return rowKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BulkLoad)) {
      return false;
    }
    BulkLoad that = (BulkLoad) o;
    return new EqualsBuilder().append(tableName, that.tableName).append(region, that.region)
      .append(columnFamily, that.columnFamily).append(hfilePath, that.hfilePath)
      .append(rowKey, that.rowKey).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(tableName).append(region).append(columnFamily)
      .append(hfilePath).append(rowKey).toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
      .append("tableName", tableName).append("region", region).append("columnFamily", columnFamily)
      .append("hfilePath", hfilePath).append("rowKey", rowKey).toString();
  }
}
