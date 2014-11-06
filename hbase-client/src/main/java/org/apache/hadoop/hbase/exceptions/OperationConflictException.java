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
package org.apache.hadoop.hbase.exceptions;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * The exception that is thrown if there's duplicate execution of non-idempotent operation.
 * Client should not retry; may use "get" to get the desired value.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OperationConflictException extends DoNotRetryIOException {
  private static final long serialVersionUID = -8930333627489862872L;

  public OperationConflictException() {
    super();
  }

  public OperationConflictException(String message) {
    super(message);
  }

  public OperationConflictException(Throwable cause) {
    super(cause);
  }

  public OperationConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
