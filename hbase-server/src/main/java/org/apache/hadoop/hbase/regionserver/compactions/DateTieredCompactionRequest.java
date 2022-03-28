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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.yetus.audience.InterfaceAudience;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EQ_DOESNT_OVERRIDE_EQUALS",
  justification="It is intended to use the same equal method as superclass")
@InterfaceAudience.Private
public class DateTieredCompactionRequest extends CompactionRequestImpl {
  private List<Long> boundaries;
  /** window start boundary to window storage policy map **/
  private Map<Long, String> boundariesPolicies;

  public DateTieredCompactionRequest(Collection<HStoreFile> files, List<Long> boundaryList,
      Map<Long, String> boundaryPolicyMap) {
    super(files);
    boundaries = boundaryList;
    boundariesPolicies = boundaryPolicyMap;
  }

  public List<Long> getBoundaries() {
    return boundaries;
  }

  public Map<Long, String> getBoundariesPolicies() {
    return boundariesPolicies;
  }

  @Override
  public String toString() {
    return super.toString() + " boundaries=" + Arrays.toString(boundaries.toArray())
      + " boundariesPolicies="+boundariesPolicies.toString();
  }
}
