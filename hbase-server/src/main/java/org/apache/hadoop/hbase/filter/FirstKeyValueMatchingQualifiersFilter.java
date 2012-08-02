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

package org.apache.hadoop.hbase.filter;

import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.mapreduce.RowCounter;

/**
 * The filter looks for the given columns in KeyValue. Once there is a match for
 * any one of the columns, it returns ReturnCode.NEXT_ROW for remaining
 * KeyValues in the row.
 * <p>
 * Note : It may emit KVs which do not have the given columns in them, if
 * these KVs happen to occur before a KV which does have a match. Given this
 * caveat, this filter is only useful for special cases like {@link RowCounter}.
 * <p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FirstKeyValueMatchingQualifiersFilter extends FirstKeyOnlyFilter {

  private Set<byte []> qualifiers;

  /**
   * This constructor should not be used.
   */
  public FirstKeyValueMatchingQualifiersFilter() {
    qualifiers = Collections.emptySet();
  }

  /**
   * Constructor which takes a set of columns. As soon as first KeyValue
   * matching any of these columns is found, filter moves to next row.
   * 
   * @param qualifiers the set of columns to me matched.
   */
  public FirstKeyValueMatchingQualifiersFilter(Set<byte []> qualifiers) {
    this.qualifiers = qualifiers;
  }

  public ReturnCode filterKeyValue(KeyValue v) {
    if (hasFoundKV()) {
      return ReturnCode.NEXT_ROW;
    } else if (hasOneMatchingQualifier(v)) {
      setFoundKV(true);
    }
    return ReturnCode.INCLUDE;
  }

  private boolean hasOneMatchingQualifier(KeyValue v) {
    for (byte[] q : qualifiers) {
      if (v.matchingQualifier(q)) {
        return true;
      }
    }
    return false;
  }

}
