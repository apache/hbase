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
package org.apache.hadoop.hbase.regionserver.handler;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;

/**
 * Handler to seek storefiles in parallel.
 */
@InterfaceAudience.Private
public class ParallelSeekHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(ParallelSeekHandler.class);
  private KeyValueScanner scanner;
  private Cell keyValue;
  private long readPoint;
  private CountDownLatch latch;
  private Throwable err = null;

  public ParallelSeekHandler(KeyValueScanner scanner,Cell keyValue, 
      long readPoint, CountDownLatch latch) {
    super(null, EventType.RS_PARALLEL_SEEK);
    this.scanner = scanner;
    this.keyValue = keyValue;
    this.readPoint = readPoint;
    this.latch = latch;
  }

  @Override
  public void process() {
    try {
      scanner.seek(keyValue);
    } catch (IOException e) {
      LOG.error("", e);
      setErr(e);
    } finally {
      latch.countDown();
    }
  }

  public Throwable getErr() {
    return err;
  }

  public void setErr(Throwable err) {
    this.err = err;
  }
}
