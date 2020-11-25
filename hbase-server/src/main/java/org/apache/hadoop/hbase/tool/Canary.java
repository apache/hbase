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

package org.apache.hadoop.hbase.tool;


import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public interface Canary {

  static Canary create(Configuration conf, ExecutorService executor) {
    return new CanaryTool(conf, executor);
  }

  @InterfaceAudience.Private
  static Canary create(Configuration conf, ExecutorService executor, CanaryTool.Sink sink) {
    return new CanaryTool(conf, executor, sink);
  }

  /**
   * Run Canary in Region mode.
   *
   * @param targets -- list of monitor tables.
   * @return the exit code of the Canary tool.
   */
  public int checkRegions(String[] targets) throws Exception;

  /**
   * Runs Canary in Region server mode.
   *
   * @param targets -- list of monitor tables.
   * @return the exit code of the Canary tool.
   */
  public int checkRegionServers(String[] targets) throws Exception;

  /**
   * Runs Canary in Zookeeper mode.
   *
   * @return the exit code of the Canary tool.
   */
  public int checkZooKeeper() throws Exception;

  public Map<String, String> getReadFailures();

  public Map<String, String> getWriteFailures();
}
