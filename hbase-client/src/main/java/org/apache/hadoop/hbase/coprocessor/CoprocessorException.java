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
package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown if a coprocessor encounters any exception.
 */
@InterfaceAudience.Public
public class CoprocessorException extends DoNotRetryIOException {
  private static final long serialVersionUID = 4357922136679804887L;

  /** Default Constructor */
  public CoprocessorException() {
    super();
  }

  /**
   * Constructor with a Class object and exception message.
   * @param clazz
   * @param s
   */
  public CoprocessorException(Class<?> clazz, String s) {
    super( "Coprocessor [" + clazz.getName() + "]: " + s);
  }

  /**
   * Constructs the exception and supplies a string as the message
   * @param s - message
   */
  public CoprocessorException(String s) {
    super(s);
  }

}
