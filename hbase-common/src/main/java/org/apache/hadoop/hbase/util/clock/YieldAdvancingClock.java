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
package org.apache.hadoop.hbase.util.clock;

import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * YieldAdvancingClock implements a strategy for currentTimeAdvancing that yields the thread
 * until the clock ticks over.
 */
@InterfaceAudience.Private
public class YieldAdvancingClock extends SpinAdvancingClock {

  public YieldAdvancingClock(HashedBytes name) {
    super(name);
  }

  // Broken out to inlinable method for subclassing and instrumentation.
  @Override
  protected void spin() throws InterruptedException {
    Thread.sleep(1, 0); // black magic to yield on all platforms
    super.spin();
  }

}
