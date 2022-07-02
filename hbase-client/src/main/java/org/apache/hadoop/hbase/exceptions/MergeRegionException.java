/*
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

import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown when something is wrong in trying to merge two regions.
 */
@InterfaceAudience.Public
public class MergeRegionException extends DoNotRetryRegionException {

  private static final long serialVersionUID = 4970899110066124122L;

  /** default constructor */
  public MergeRegionException() {
    super();
  }

  /**
   * Constructor
   * @param s message
   */
  public MergeRegionException(String s) {
    super(s);
  }

  public MergeRegionException(Throwable cause) {
    super(cause);
  }
}
