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

import org.apache.hadoop.hbase.regionserver.StoreFile;

public class DateTieredCompactionRequest extends CompactionRequest {
  private List<Long> boundaries;
  
  public DateTieredCompactionRequest(Collection<StoreFile> files, List<Long> boundaryList) {
    super(files);
    boundaries = boundaryList;
  }
  
  public List<Long> getBoundaries() {
    return boundaries;
  }
  
  @Override
  public String toString() {
    return super.toString() + " boundaries=" + Arrays.toString(boundaries.toArray());
  }
}
