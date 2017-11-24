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


@InterfaceAudience.Private
public final class ExtendedCellBuilderFactory {

  public static ExtendedCellBuilder create(CellBuilderType type) {
    return create(type, true);
  }

  /**
   * Allows creating a cell with the given CellBuilderType.
   * @param type the type of CellBuilder(DEEP_COPY or SHALLOW_COPY).
   * @param allowSeqIdUpdate if seqId can be updated. CPs are not allowed to update
   *        the seqId
   * @return the cell that is created
   */
  public static ExtendedCellBuilder create(CellBuilderType type, boolean allowSeqIdUpdate) {
    switch (type) {
      case SHALLOW_COPY:
        // CPs are not allowed to update seqID and they always use DEEP_COPY. So we have not
        // passing 'allowSeqIdUpdate' to IndividualBytesFieldCellBuilder
        return new IndividualBytesFieldCellBuilder();
      case DEEP_COPY:
        return new KeyValueBuilder(allowSeqIdUpdate);
      default:
        throw new UnsupportedOperationException("The type:" + type + " is unsupported");
    }
  }

  private ExtendedCellBuilderFactory(){
  }
}
