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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Returned to the clients when their request was discarded due to server being overloaded.
 * Clients should retry upon receiving it.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
public class CallDroppedException extends HBaseServerException {
  public CallDroppedException() {
    // For now all call drops are due to server being overloaded.
    // We could decouple this if desired.
    super(true);
  }

  // Absence of this constructor prevents proper unwrapping of
  // remote exception on the client side
  public CallDroppedException(String message) {
    super(true, message);
  }
}
