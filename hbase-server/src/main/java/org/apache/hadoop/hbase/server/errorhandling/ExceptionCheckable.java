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
 * Check for errors to a given process.
 * @param <E> Type of error that <tt>this</tt> throws if it finds an error
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ExceptionCheckable<E extends Exception> {

  /**
   * Checks to see if any process to which the exception checker is bound has created an error that
   * would cause a failure.
   * @throws E if there has been an error, allowing a fail-fast mechanism
   */
  public void failOnError() throws E;

  /**
   * Non-exceptional form of {@link #failOnError()}. Checks to see if any process to which the
   * exception checkers is bound has created an error that would cause a failure.
   * @return <tt>true</tt> if there has been an error,<tt>false</tt> otherwise
   */
  public boolean checkForError();
}