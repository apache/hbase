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

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Basic strategy chooses between two actions: flattening a segment or merging indices of all
 * segments in the pipeline.
 * If number of segments in pipeline exceed the limit defined in MemStoreCompactionStrategy then
 * apply merge, otherwise flatten some segment.
 */
@InterfaceAudience.Private
public class BasicMemStoreCompactionStrategy extends MemStoreCompactionStrategy{
  private static final String NAME = "BASIC";

  public BasicMemStoreCompactionStrategy(Configuration conf, String cfName) {
    super(conf, cfName);
  }

  @Override
  public Action getAction(VersionedSegmentsList versionedList) {
    return simpleMergeOrFlatten(versionedList, getName());
  }

  @Override
  protected String getName() {
    return NAME;
  }
}
