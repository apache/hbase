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
package org.apache.hadoop.hbase.procedure2;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Indicate that a procedure wants to be rescheduled. Usually because there are something wrong but
 * we do not want to fail the procedure.
 * <p>
 * TODO: need to support scheduling after a delay.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class ProcedureYieldException extends ProcedureException {

  /** default constructor */
  public ProcedureYieldException() {
    super();
  }

  /**
   * Constructor
   * @param s message
   */
  public ProcedureYieldException(String s) {
    super(s);
  }
}
