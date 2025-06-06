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
package org.apache.hadoop.hbase.regionserver.handler;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.io.hfile.HFileReaderImpl;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler to seek storefiles in parallel.
 */
@InterfaceAudience.Private
public class ParallelSeekHandler extends EventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ParallelSeekHandler.class);
  private KeyValueScanner scanner;
  private ExtendedCell keyValue;
  private long readPoint;
  private CountDownLatch latch;
  private Throwable err = null;
  private final AtomicInteger bytesReadFromFs;
  private final AtomicInteger bytesReadFromCache;

  public ParallelSeekHandler(KeyValueScanner scanner, ExtendedCell keyValue, long readPoint,
    CountDownLatch latch, AtomicInteger bytesReadFromFs, AtomicInteger bytesReadFromCache) {
    super(null, EventType.RS_PARALLEL_SEEK);
    this.scanner = scanner;
    this.keyValue = keyValue;
    this.readPoint = readPoint;
    this.latch = latch;
    this.bytesReadFromFs = bytesReadFromFs;
    this.bytesReadFromCache = bytesReadFromCache;
  }

  @Override
  public void process() {
    try {
      scanner.seek(keyValue);
      bytesReadFromFs.addAndGet(HFileReaderImpl.bytesReadFromFs.get().getAndSet(0));
      bytesReadFromCache.addAndGet(HFileReaderImpl.bytesReadFromCache.get().getAndSet(0));
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
