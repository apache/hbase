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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Subclass if exception is not meant to be retried: e.g.
 * {@link org.apache.hadoop.hbase.UnknownScannerException}
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DoNotRetryIOException extends HBaseIOException {
  // TODO: This would be more useful as a marker interface than as a class.
  private static final long serialVersionUID = 1197446454511704139L;

  /**
   * default constructor
   */
  public DoNotRetryIOException() {
    super();
  }

  /**
   * @param message
   */
  public DoNotRetryIOException(String message) {
    super(message);
  }

  /**
   * @param message
   * @param cause
   */
  public DoNotRetryIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public DoNotRetryIOException(Throwable cause) {
    super(cause);
  }
}
