/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.manual.utils;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;


public abstract class MultiThreadedAction
{
  private static final Log LOG = LogFactory.getLog(MultiThreadedAction.class);
  public static int numThreads_ = 1;
  public static byte[] tableName_;
  public static byte[] columnFamily_;
  public float verifyPercent_ = 0;
  public long startKey_ = 0;
  public long endKey_ = 1;
  public AtomicInteger numThreadsWorking_ = new AtomicInteger(0);
  public AtomicLong numKeys_ = new AtomicLong(0);
  public AtomicLong numKeysVerified_ = new AtomicLong(0);
  public AtomicLong numCols_ = new AtomicLong(0);
  public AtomicLong numErrors_ = new AtomicLong(0);
  public AtomicLong numOpFailures_ = new AtomicLong(0);
  public AtomicLong cumulativeOpTime_ = new AtomicLong(0);
  public boolean verbose_ = false;
  public Random random_ = new Random();
  public HBaseConfiguration conf_;

  public void startReporter(String id) {
    (new ProgressReporter(id)).start();
  }

  public class ProgressReporter extends Thread {

     private String id_ = "";

     public ProgressReporter(String id) {
       id_ = id;
     }

    public void run() {
      long startTime = System.currentTimeMillis();
      long reportingInterval = 5000;

      long priorNumKeys = 0;
      long priorCumulativeOpTime = 0;

      while(numThreadsWorking_.get() != 0) {
        String threadsLeft = "[" + id_ + ":" + numThreadsWorking_.get() + "] ";
        if(numKeys_.get() == 0) {
          LOG.info(threadsLeft + "Number of keys = 0");
        }
        else {
          long numKeys = numKeys_.get();
          long time = System.currentTimeMillis() - startTime;
          long cumulativeOpTime = cumulativeOpTime_.get();

          long numKeysDelta = numKeys - priorNumKeys;
          long cumulativeOpTimeDelta = cumulativeOpTime - priorCumulativeOpTime;

          LOG.info(threadsLeft + "Keys = " + numKeys +
                   ", cols = " + DisplayFormatUtils.formatNumber(numCols_.get()) +
                   ", time = " + DisplayFormatUtils.formatTime(time) +
                   ((numKeys > 0 && time > 0)? (" Overall: [" +
                                               "keys/s = " + numKeys*1000/time +
                                               ", latency = " + cumulativeOpTime/numKeys + " ms]")
                                             : "") +
                   ((numKeysDelta > 0) ? (" Current: [" +
                                         "keys/s = " + numKeysDelta*1000/reportingInterval +
                                         ", latency = " + cumulativeOpTimeDelta/numKeysDelta + " ms]")
                                      : "") +
                   ((numKeysVerified_.get()>0)?(", verified = " + numKeysVerified_.get()):"") +
                   ((numOpFailures_.get()>0)?(", FAILURES = " + numOpFailures_.get()):"") +
                   ((numErrors_.get()>0)?(", ERRORS = " + numErrors_.get()):"")
                   );

          priorNumKeys = numKeys;
          priorCumulativeOpTime = cumulativeOpTime;
        }
        try {
          Thread.sleep(reportingInterval);
        }
        catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public abstract void start(long startKey, long endKey, int numThreads);
}
