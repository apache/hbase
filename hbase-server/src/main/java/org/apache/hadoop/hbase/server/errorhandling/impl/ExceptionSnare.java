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
package org.apache.hadoop.hbase.server.errorhandling.impl;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionListener;
import org.apache.hadoop.hbase.server.errorhandling.Name;
import org.apache.hadoop.hbase.server.errorhandling.exception.UnknownErrorException;

/**
 * Simple exception handler that keeps track of whether of its failure state, and the exception that
 * should be thrown based on the received error.
 * <p>
 * Ensures that an exception is not propagated if an error has already been received, ensuring that
 * you don't have infinite error propagation.
 * <p>
 * You can think of it like a 'one-time-use' {@link ExceptionCheckable}, that once it receives an
 * error will not listen to any new error updates.
 * <p>
 * Thread-safe.
 * @param <E> Type of exception to throw when calling {@link #failOnError()}
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ExceptionSnare<E extends Exception> implements ExceptionCheckable<E>,
    ExceptionListener<E> {

  private static final Log LOG = LogFactory.getLog(ExceptionSnare.class);
  private boolean error = false;
  protected E exception;
  protected Name name;

  /**
   * Create an exception snare with a generic error name
   */
  public ExceptionSnare() {
    this.name = new Name("generic-error-snare");
  }

  @Override
  public void failOnError() throws E {
    if (checkForError()) {
      if (exception == null) throw new UnknownErrorException();
      throw exception;
    }
  }

  @Override
  public boolean checkForError() {
    return this.error;
  }

  @Override
  public void receiveError(String message, E e, Object... info) {
    LOG.error(name.getNamePrefixForLog() + "Got an error:" + message + ", info:"
        + Arrays.toString(info));
    receiveInternalError(e);
  }

  /**
   * Receive an error notification from internal sources. Can be used by subclasses to set an error.
   * <p>
   * This method may be called concurrently, so precautions must be taken to not clobber yourself,
   * either making the method <tt>synchronized</tt>, synchronizing on <tt>this</tt> of calling this
   * method.
   * @param e exception that caused the error (can be null).
   */
  protected synchronized void receiveInternalError(E e) {
    // if we already got the error or we received the error fail fast
    if (this.error) return;
    // store the error since we haven't seen it before
    this.error = true;
    this.exception = e;
  }
}