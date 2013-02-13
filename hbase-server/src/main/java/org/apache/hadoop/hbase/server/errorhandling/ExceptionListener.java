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
package org.apache.hadoop.hbase.server.errorhandling;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Listen for errors on a process or operation
 * @param <E> Type of exception that is expected
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ExceptionListener<E extends Exception> {

  /**
   * Receive an error.
   * <p>
   * Implementers must ensure that this method is thread-safe.
   * @param message reason for the error
   * @param e exception causing the error
   * @param info general information about the error
   */
  public void receiveError(String message, E e, Object... info);
}