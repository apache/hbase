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
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionOrchestrator;

/**
 * Simple visitor interface to update an error listener with an error notification
 * @see ExceptionOrchestrator
 * @param <T> Type of listener to update
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface ExceptionVisitor<T> {

  /**
   * Visit the listener with the given error, possibly transforming or ignoring the error
   * @param listener listener to update
   * @param message error message
   * @param e exception that caused the error
   * @param info general information about the error
   */
  public void visit(T listener, String message, Exception e, Object... info);
}