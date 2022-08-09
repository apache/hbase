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
package org.apache.hadoop.hbase.io.hfile;

import java.util.concurrent.atomic.LongAdder;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class BloomFilterMetrics {

  private final LongAdder eligibleRequests = new LongAdder();
  private final LongAdder requests = new LongAdder();
  private final LongAdder negativeResults = new LongAdder();

  /**
   * Increment bloom request count, and negative result count if !passed
   */
  public void incrementRequests(boolean passed) {
    requests.increment();
    if (!passed) {
      negativeResults.increment();
    }
  }

  /**
   * Increment for cases where bloom filter could have been used but wasn't defined or loaded.
   */
  public void incrementEligible() {
    eligibleRequests.increment();
  }

  /** Returns Current value for bloom requests count */
  public long getRequestsCount() {
    return requests.sum();
  }

  /** Returns Current value for bloom negative results count */
  public long getNegativeResultsCount() {
    return negativeResults.sum();
  }

  /**
   * Returns Current value for requests which could have used bloom filters but wasn't defined or
   * loaded.
   */
  public long getEligibleRequestsCount() {
    return eligibleRequests.sum();
  }

}
