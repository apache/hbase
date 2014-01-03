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
package org.apache.hadoop.hbase.security.visibility;

import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.util.Pair;

/**
 * This Filter checks the visibility expression with each KV against visibility labels associated
 * with the scan. Based on the check the KV is included in the scan result or gets filtered out.
 */
@InterfaceAudience.Private
class VisibilityLabelFilter extends FilterBase {

  private BitSet authLabels;

  public VisibilityLabelFilter(BitSet authLabels) {
    this.authLabels = authLabels;
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) throws IOException {
    Iterator<Tag> tagsItr = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(),
        cell.getTagsLength());
    boolean visibilityTagPresent = false;
    while (tagsItr.hasNext()) {
      boolean includeKV = true;
      Tag tag = tagsItr.next();
      if (tag.getType() == VisibilityUtils.VISIBILITY_TAG_TYPE) {
        visibilityTagPresent = true;
        int offset = tag.getTagOffset();
        int endOffset = offset + tag.getTagLength();
        while (offset < endOffset) {
          Pair<Integer, Integer> result = StreamUtils.readRawVarint32(tag.getBuffer(), offset);
          int currLabelOrdinal = result.getFirst();
          if (currLabelOrdinal < 0) {
            // check for the absence of this label in the Scan Auth labels
            // ie. to check BitSet corresponding bit is 0
            int temp = -currLabelOrdinal;
            if (this.authLabels.get(temp)) {
              includeKV = false;
              break;
            }
          } else {
            if (!this.authLabels.get(currLabelOrdinal)) {
              includeKV = false;
              break;
            }
          }
          offset += result.getSecond();
        }
        if (includeKV) {
          // We got one visibility expression getting evaluated to true. Good to include this KV in
          // the result then.
          return ReturnCode.INCLUDE;
        }
      }
    }
    return visibilityTagPresent ? ReturnCode.SKIP : ReturnCode.INCLUDE;
  }
}