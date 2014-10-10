/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.rest;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;

import org.apache.hadoop.hbase.rest.MetricsRESTSource;

@InterfaceAudience.Private
public class MetricsREST {

  public MetricsRESTSource getSource() {
    return source;
  }

  private MetricsRESTSource source;

  public MetricsREST() {
     source = CompatibilitySingletonFactory.getInstance(MetricsRESTSource.class);
  }
  
  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final int inc) {
    source.incrementRequests(inc);
  }
  
  /**
   * @param inc How much to add to sucessfulGetCount.
   */
  public void incrementSucessfulGetRequests(final int inc) {
    source.incrementSucessfulGetRequests(inc);
  }
  
  /**
   * @param inc How much to add to sucessfulPutCount.
   */
  public void incrementSucessfulPutRequests(final int inc) {
    source.incrementSucessfulPutRequests(inc);
  }

  /**
   * @param inc How much to add to failedPutCount.
   */
  public void incrementFailedPutRequests(final int inc) {
    source.incrementFailedPutRequests(inc);
  }
  
  /**
   * @param inc How much to add to failedGetCount.
   */
  public void incrementFailedGetRequests(final int inc) {
    source.incrementFailedGetRequests(inc);
  }

  /**
   * @param inc How much to add to sucessfulDeleteCount.
   */
  public void incrementSucessfulDeleteRequests(final int inc) {
    source.incrementSucessfulDeleteRequests(inc);
  }

  /**
   * @param inc How much to add to failedDeleteCount.
   */
  public void incrementFailedDeleteRequests(final int inc) {
    source.incrementFailedDeleteRequests(inc);
  }

  /**
   * @param inc How much to add to sucessfulScanCount.
   */
  public synchronized void incrementSucessfulScanRequests(final int inc) {
    source.incrementSucessfulScanRequests(inc);
  }

  /**
   * @param inc How much to add to failedScanCount.
   */
  public void incrementFailedScanRequests(final int inc) {
    source.incrementFailedScanRequests(inc);
  }

}
