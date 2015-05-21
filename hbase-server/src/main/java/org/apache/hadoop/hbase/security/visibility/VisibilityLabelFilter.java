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
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SimpleMutableByteRange;

/**
 * This Filter checks the visibility expression with each KV against visibility labels associated
 * with the scan. Based on the check the KV is included in the scan result or gets filtered out.
 */
@InterfaceAudience.Private
class VisibilityLabelFilter extends FilterBase {

  private final VisibilityExpEvaluator expEvaluator;
  private final Map<ByteRange, Integer> cfVsMaxVersions;
  private final ByteRange curFamily;
  private final ByteRange curQualifier;
  private int curFamilyMaxVersions;
  private int curQualMetVersions;

  public VisibilityLabelFilter(VisibilityExpEvaluator expEvaluator,
      Map<ByteRange, Integer> cfVsMaxVersions) {
    this.expEvaluator = expEvaluator;
    this.cfVsMaxVersions = cfVsMaxVersions;
    this.curFamily = new SimpleMutableByteRange();
    this.curQualifier = new SimpleMutableByteRange();
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    // Impl in FilterBase might do unnecessary copy for Off heap backed Cells.
    return false;
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) throws IOException {
    if (curFamily.getBytes() == null
        || (Bytes.compareTo(curFamily.getBytes(), curFamily.getOffset(), curFamily.getLength(),
            cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) != 0)) {
      curFamily.set(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
      // For this family, all the columns can have max of curFamilyMaxVersions versions. No need to
      // consider the older versions for visibility label check.
      // Ideally this should have been done at a lower layer by HBase (?)
      curFamilyMaxVersions = cfVsMaxVersions.get(curFamily);
      // Family is changed. Just unset curQualifier.
      curQualifier.unset();
    }
    if (curQualifier.getBytes() == null
        || (Bytes.compareTo(curQualifier.getBytes(), curQualifier.getOffset(),
            curQualifier.getLength(), cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength()) != 0)) {
      curQualifier.set(cell.getQualifierArray(), cell.getQualifierOffset(),
          cell.getQualifierLength());
      curQualMetVersions = 0;
    }
    curQualMetVersions++;
    if (curQualMetVersions > curFamilyMaxVersions) {
      return ReturnCode.SKIP;
    }

    return this.expEvaluator.evaluate(cell) ? ReturnCode.INCLUDE : ReturnCode.SKIP;
  }

  @Override
  public void reset() throws IOException {
    this.curFamily.unset();
    this.curQualifier.unset();
    this.curFamilyMaxVersions = 0;
    this.curQualMetVersions = 0;
  }
}
