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
package org.apache.hadoop.hbase.security;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

@InterfaceAudience.Private
public class SecureBulkLoadUtil {
  private final static String BULKLOAD_STAGING_DIR = "hbase.bulkload.staging.dir";

  /**
   * This returns the staging path for a given column family.
   * This is needed for clean recovery and called reflectively in LoadIncrementalHFiles
   */
  public static Path getStagingPath(Configuration conf, String bulkToken, byte[] family) {
    Path stageP = new Path(getBaseStagingDir(conf), bulkToken);
    return new Path(stageP, Bytes.toString(family));
  }

  public static Path getBaseStagingDir(Configuration conf) {
    return new Path(conf.get(BULKLOAD_STAGING_DIR, "/tmp/hbase-staging"));
  }
}
