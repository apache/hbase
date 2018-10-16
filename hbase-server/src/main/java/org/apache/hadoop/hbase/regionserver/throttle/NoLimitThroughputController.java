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
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class NoLimitThroughputController implements ThroughputController {

  public static final NoLimitThroughputController INSTANCE = new NoLimitThroughputController();

  @Override
  public void setup(RegionServerServices server) {
  }

  @Override
  public void start(String compactionName) {
  }

  @Override
  public long control(String compactionName, long size) throws InterruptedException {
    return 0;
  }

  @Override
  public void finish(String compactionName) {
  }

  private boolean stopped;

  @Override
  public void stop(String why) {
    stopped = true;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public String toString() {
    return "NoLimitThroughputController";
  }
}
