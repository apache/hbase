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
package org.apache.hadoop.hbase.regionserver.throttle;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * A utility that constrains the total throughput of one or more simultaneous flows by
 * sleeping when necessary.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public interface ThroughputController extends Stoppable {

  /**
   * Setup controller for the given region server.
   */
  void setup(RegionServerServices server);

  /**
   * Start the throughput controller.
   */
  void start(String name);

  /**
   * Control the throughput. Will sleep if too fast.
   * @return the actual sleep time.
   */
  long control(String name, long size) throws InterruptedException;

  /**
   * Finish the controller. Should call this method in a finally block.
   */
  void finish(String name);
}
