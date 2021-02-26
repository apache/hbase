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
package org.apache.hadoop.hbase.client;

import org.apache.yetus.audience.InterfaceAudience;

import java.io.InterruptedIOException;
import java.util.List;

/**
 * The context used to wait for results from one submit call. If submit call is made with
 * needResults false, results will not be saved.
 * @since 2.0.0
 */
@InterfaceAudience.Private
public interface AsyncRequestFuture {
  public boolean hasError();
  public RetriesExhaustedWithDetailsException getErrors();
  public List<? extends Row> getFailedOperations();
  public Object[] getResults() throws InterruptedIOException;
  /** Wait until all tasks are executed, successfully or not. */
  public void waitUntilDone() throws InterruptedIOException;
}
