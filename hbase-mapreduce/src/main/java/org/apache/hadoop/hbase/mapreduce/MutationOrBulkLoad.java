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
package org.apache.hadoop.hbase.mapreduce;

import java.util.List;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MutationOrBulkLoad {

  private Mutation mutation;
  private List<String> bulkLoadFiles;

  // Private constructor to enforce the use of factory methods
  private MutationOrBulkLoad(Mutation mutation, List<String> bulkLoadFiles) {
    this.mutation = mutation;
    this.bulkLoadFiles = bulkLoadFiles;
  }

  // Factory method to create an instance with a Mutation
  public static MutationOrBulkLoad fromMutation(Mutation mutation) {
    if (mutation == null) {
      throw new IllegalArgumentException("Mutation cannot be null");
    }
    return new MutationOrBulkLoad(mutation, null);
  }

  // Factory method to create an instance with a list of bulk load files
  public static MutationOrBulkLoad fromBulkLoadFiles(List<String> bulkLoadFiles) {
    if (bulkLoadFiles == null || bulkLoadFiles.isEmpty()) {
      throw new IllegalArgumentException("Bulk load files cannot be null or empty");
    }
    return new MutationOrBulkLoad(null, bulkLoadFiles);
  }

  public Mutation getMutation() {
    return mutation;
  }

  public List<String> getBulkLoadFiles() {
    return bulkLoadFiles;
  }
}
