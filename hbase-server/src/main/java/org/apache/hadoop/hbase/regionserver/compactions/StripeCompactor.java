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

import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.Store;

/**
 * This is the placeholder for stripe compactor. The implementation,
 * as well as the proper javadoc, will be added in HBASE-7967.
 */
public class StripeCompactor extends Compactor {

  public StripeCompactor(Configuration conf, final Store store) {
    super(conf, store);
  }

  public List<Path> compact(CompactionRequest request, List<byte[]> targetBoundaries,
      byte[] dropDeletesFromRow, byte[] dropDeletesToRow) {
    throw new NotImplementedException();
  }

  public List<Path> compact(CompactionRequest request, int targetCount, long targetSize,
      byte[] left, byte[] right, byte[] dropDeletesFromRow, byte[] dropDeletesToRow) {
    throw new NotImplementedException();
  }
}
