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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Compare two HBase cells inner store, skip compare family for better performance.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class InnerStoreCellComparator extends CellComparatorImpl {

  private static final long serialVersionUID = 8186411895799094989L;

  public static final InnerStoreCellComparator
    INNER_STORE_COMPARATOR = new InnerStoreCellComparator();

  protected int compareFamilies(Cell left, int leftFamilyLength, Cell right,
    int rightFamilyLength) {
    if (leftFamilyLength == 0 || rightFamilyLength == 0) {
      return super.compareFamilies(left, leftFamilyLength, right, rightFamilyLength);
    }
    return 0;
  }

  protected int compareFamilies(KeyValue left, KeyValue right, int leftFamilyLength,
    int rightFamilyLength, int leftFamilyPosition, int rightFamilyPosition) {
    if (leftFamilyLength == 0 || rightFamilyLength == 0) {
      return super.compareFamilies(left, right, leftFamilyLength, rightFamilyLength,
        leftFamilyPosition, rightFamilyPosition);
    }
    return 0;
  }

  protected int compareFamilies(ByteBufferKeyValue left, ByteBufferKeyValue right,
    int leftFamilyLength, int rightFamilyLength, int leftFamilyPosition, int rightFamilyPosition) {
    if (leftFamilyLength == 0 || rightFamilyLength == 0) {
      return super.compareFamilies(left, right, leftFamilyLength, rightFamilyLength,
        leftFamilyPosition, rightFamilyPosition);
    }
    return 0;
  }

  protected int compareFamilies(KeyValue left, ByteBufferKeyValue right, int leftFamilyLength,
    int rightFamilyLength, int leftFamilyPosition, int rightFamilyPosition) {
    if (leftFamilyLength == 0 || rightFamilyLength == 0) {
      return super.compareFamilies(left, right, leftFamilyLength, rightFamilyLength,
        leftFamilyPosition, rightFamilyPosition);
    }
    return 0;
  }
}
