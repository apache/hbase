/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.security.access;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.security.TokenInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.List;

/**
 * Provides a secure way to bulk load data onto HBase
 * These are internal API. Bulk load should be initiated
 * via {@link org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles}
 * with security enabled.
 */
@TokenInfo("HBASE_AUTH_TOKEN")
public interface SecureBulkLoadProtocol extends CoprocessorProtocol {

  /**
   * Prepare for bulk load.
   * Will be called before bulkLoadHFiles()
   * @param tableName
   * @return a bulkToken which uniquely identifies the bulk session
   * @throws IOException
   */
  String prepareBulkLoad(byte[] tableName) throws IOException;

  /**
   * Cleanup after bulk load.
   * Will be called after bulkLoadHFiles().
   * @param bulkToken
   * @throws IOException
   */
  void cleanupBulkLoad(String bulkToken) throws IOException;

  /**
   * Secure version of HRegionServer.bulkLoadHFiles().
   * @param familyPaths column family to HFile path pairs
   * @param userToken requesting user's HDFS delegation token
   * @param bulkToken
   * @return
   * @throws IOException
   */
  boolean bulkLoadHFiles(List<Pair<byte[], String>> familyPaths,
                         Token<?> userToken, String bulkToken) throws IOException;

}
