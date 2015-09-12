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

package org.apache.hadoop.hbase;

import java.io.IOException;


/**
 * A compatibility shim layer for interacting with different versions of Hadoop.
 */
//NOTE: we can move this under src/main if main code wants to use this shim layer
public interface HadoopShims {

  /**
   * Returns a TaskAttemptContext instance created from the given parameters.
   * @param job an instance of o.a.h.mapreduce.Job
   * @param taskId an identifier for the task attempt id. Should be parsable by
   * TaskAttemptId.forName()
   * @return a concrete TaskAttemptContext instance of o.a.h.mapreduce.TaskAttemptContext
   */
  <T,J> T createTestTaskAttemptContext(final J job, final String taskId);

  /**
   * Returns an array of DatanodeInfo for all live datanodes in the cluster
   * @param dfs instance of DistributedFileSystem
   * @return
   */
  <I,DFS> I[] getLiveDatanodes(DFS dfs) throws IOException;
}
