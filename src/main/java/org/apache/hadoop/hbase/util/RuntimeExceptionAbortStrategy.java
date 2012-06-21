/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;

public class RuntimeExceptionAbortStrategy implements Abortable {
  private volatile boolean aborted = false;

  private static final Log LOG = LogFactory.getLog(RuntimeExceptionAbortStrategy.class);

  public static final RuntimeExceptionAbortStrategy INSTANCE = new RuntimeExceptionAbortStrategy(); 

  @Override
  public void abort(String why, Throwable e) {
    aborted = true;
    LOG.error(why, e);
    throw new RuntimeException("Abort as Runtime exception because " + why, e);
  }

  @Override
  public boolean isAborted() {
    return aborted;
  }
}
