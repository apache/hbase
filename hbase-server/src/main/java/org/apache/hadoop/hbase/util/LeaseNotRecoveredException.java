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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Thrown when the lease was expected to be recovered,
 * but the file can't be opened.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LeaseNotRecoveredException extends HBaseIOException {
  public LeaseNotRecoveredException() {
    super();
  }

  public LeaseNotRecoveredException(String message) {
    super(message);
  }

  public LeaseNotRecoveredException(String message, Throwable cause) {
      super(message, cause);
  }

  public LeaseNotRecoveredException(Throwable cause) {
      super(cause);
  }
}
