/*
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
/**
 * 
 * This class stores the Operation status code and the exception message
 * that occurs in case of failure of operations like put, delete, etc.
 * This class is added with a purpose of adding more details or info regarding
 * the operation status in future.
 *
 */
@InterfaceAudience.Private
public class OperationStatus {

  /** Singleton for successful operations.  */
  static final OperationStatus SUCCESS =
    new OperationStatus(OperationStatusCode.SUCCESS);

  /** Singleton for failed operations.  */
  static final OperationStatus FAILURE =
    new OperationStatus(OperationStatusCode.FAILURE);

  /** Singleton for operations not yet run.  */
  static final OperationStatus NOT_RUN =
    new OperationStatus(OperationStatusCode.NOT_RUN);

  private final OperationStatusCode code;

  private final String exceptionMsg;

  public OperationStatus(OperationStatusCode code) {
    this(code, "");
  }

  public OperationStatus(OperationStatusCode code, String exceptionMsg) {
    this.code = code;
    this.exceptionMsg = exceptionMsg;
  }

  public OperationStatus(OperationStatusCode code, Exception e) {
    this.code = code;
    this.exceptionMsg = (e == null) ? "" : e.getClass().getName() + ": " + e.getMessage();
  }

  /**
   * @return OperationStatusCode
   */
  public OperationStatusCode getOperationStatusCode() {
    return code;
  }

  /**
   * @return ExceptionMessge
   */
  public String getExceptionMsg() {
    return exceptionMsg;
  }
}
