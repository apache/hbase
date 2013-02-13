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
package org.apache.hadoop.hbase.snapshot;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Utility class to help manage {@link SnapshotDescription SnapshotDesriptions}.
 */
public class SnapshotDescriptionUtils {

  private SnapshotDescriptionUtils() {
    // private constructor for utility class
  }
  
  /**
   * Check to make sure that the description of the snapshot requested is valid
   * @param snapshot description of the snapshot
   * @throws IllegalArgumentException if the name of the snapshot or the name of the table to
   *           snapshot are not valid names.
   */
  public static void assertSnapshotRequestIsValid(SnapshotDescription snapshot)
      throws IllegalArgumentException {
    // FIXME these method names is really bad - trunk will probably change
    // make sure the snapshot name is valid
    HTableDescriptor.isLegalTableName(Bytes.toBytes(snapshot.getName()));
    // make sure the table name is valid
    HTableDescriptor.isLegalTableName(Bytes.toBytes(snapshot.getTable()));
  }
}