/**
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.mob.MobUtils;

/**
 * A filter that returns the cells which have mob reference tags. It's a server-side filter.
 */
@InterfaceAudience.Private
class MobReferenceOnlyFilter extends FilterBase {

  @Override
  public ReturnCode filterKeyValue(Cell cell) {
    if (null != cell) {
      // If a cell with a mob reference tag, it's included.
      if (MobUtils.isMobReferenceCell(cell)) {
        return ReturnCode.INCLUDE;
      }
    }
    return ReturnCode.SKIP;
  }
}
