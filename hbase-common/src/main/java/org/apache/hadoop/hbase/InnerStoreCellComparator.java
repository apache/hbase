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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Compare two HBase cells inner store, skip compare family for better performance. Important!!!
 * We should not make fake cell with fake family which length greater than zero inner store,
 * otherwise this optimization cannot be used.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class InnerStoreCellComparator extends CellComparatorImpl {

  private static final long serialVersionUID = 8186411895799094989L;

  public static final InnerStoreCellComparator INNER_STORE_COMPARATOR =
      new InnerStoreCellComparator();

  // 不再重写 compareFamilies，由父类 CellComparatorImpl 处理 RVV 优化逻辑

  /**
   * Utility method that makes a guess at comparator to use based off passed tableName. Use in
   * extreme when no comparator specified.
   * @return CellComparator to use going off the {@code tableName} passed.
   */
  public static CellComparator getInnerStoreCellComparator(TableName tableName) {
    return getInnerStoreCellComparator(tableName.toBytes());
  }

  /**
   * Utility method that makes a guess at comparator to use based off passed tableName. Use in
   * extreme when no comparator specified.
   * @return CellComparator to use going off the {@code tableName} passed.
   */
  public static CellComparator getInnerStoreCellComparator(byte[] tableName) {
    return Bytes.equals(tableName, TableName.META_TABLE_NAME.toBytes())
        ? MetaCellComparator.META_COMPARATOR
        : InnerStoreCellComparator.INNER_STORE_COMPARATOR;
  }
}
