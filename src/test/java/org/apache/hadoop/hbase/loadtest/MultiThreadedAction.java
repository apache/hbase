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
package org.apache.hadoop.hbase.loadtest;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public abstract class MultiThreadedAction
{
  private static final Log LOG = LogFactory.getLog(MultiThreadedAction.class);
  public int numThreads = 1;
  public static byte[] tableName;
  public float verifyPercent = 0;
  public long startKey = 0;
  public long endKey = 1;
  public AtomicInteger numThreadsWorking = new AtomicInteger(0);
  public AtomicLong numRows_ = new AtomicLong(0);
  public AtomicLong numRowsVerified_ = new AtomicLong(0);
  public AtomicLong numKeys_ = new AtomicLong(0);
  public AtomicLong numErrors_ = new AtomicLong(0);
  public AtomicLong numOpFailures_ = new AtomicLong(0);
  public AtomicLong cumulativeOpTime_ = new AtomicLong(0);
  private boolean verbose = false;
  protected Random random = new Random();
  public Configuration conf;
  private boolean shouldRun = true;
  private ColumnFamilyProperties[] familyProperties;
  private boolean stopOnError = false;
  public ProgressReporter currentProgressReporter = null;

  public void startReporter(String id) {
    currentProgressReporter = new ProgressReporter(id);
    currentProgressReporter.start();
  }

  public boolean getStopOnError() {
    return stopOnError;
  }

  public void setStopOnError(boolean stopOnError) {
    this.stopOnError = stopOnError;
  }

  public class ProgressReporter extends Thread {

    private String id_ = "";

    public ProgressReporter(String id) {
      id_ = id;
    }

    public void run() {
      long startTime = System.currentTimeMillis();
      long reportingInterval = 5000;

      long priorNumRows = 0;
      long priorCumulativeOpTime = 0;

      while(verbose && numThreadsWorking.get() != 0) {
        String threadsLeft = "[" + id_ + ":" + numThreadsWorking.get() + "] ";
        if(MultiThreadedAction.this.numRows_.get() == 0) {
          LOG.info(threadsLeft + "Number of rows = 0");
        }
        else {
          long numRowsNumber = numRows_.get();
          long time = System.currentTimeMillis() - startTime;
          long cumulativeOpTime = cumulativeOpTime_.get();

          long numRowsDelta = numRowsNumber - priorNumRows;
          long cumulativeOpTimeDelta = cumulativeOpTime - priorCumulativeOpTime;

          LOG.info(threadsLeft + "Rows = " + numRowsNumber +
              ", keys = " + DisplayFormatUtils.formatNumber(numKeys_.get()) +
              ", time = " + DisplayFormatUtils.formatTime(time) +
              ((numRowsNumber > 0 && time > 0)? 
                  (" Overall: [" +
                      "keys/s = " + numRowsNumber*1000/time +
                      ", latency = " + cumulativeOpTime/numRowsNumber + " ms]")
                      : "") +
                      ((numRowsDelta > 0) ? 
                          (" Current: [" +
                          "rows/s = " + numRowsDelta*1000/reportingInterval +
                          ", latency = " + cumulativeOpTimeDelta/numRowsDelta +
                          " ms]") : "") +
                          ((numRowsVerified_.get()>0)?(", verified = " +
                              numRowsVerified_.get()):"") +
                              ((numOpFailures_.get()>0)?(", FAILURES = " +
                                  numOpFailures_.get()):"") +
                                  ((numErrors_.get()>0)?(", ERRORS = " +
                                      numErrors_.get()):"")
          );
          priorNumRows = numRowsNumber;
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

  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }
  
  public boolean getVerbose() {
    return this.verbose;
  }

  public void setColumnFamilyProperties(
      ColumnFamilyProperties[] familyProperties) {
    this.familyProperties = familyProperties;
  }

  public ColumnFamilyProperties[] getColumnFamilyProperties() {
    return this.familyProperties;
  }

  public void setVerficationPercent(float verifyPercent) {
    this.verifyPercent = verifyPercent;
  }

  public boolean shouldContinueRunning() {
    return shouldRun;
  }

  /**
   * This is an unsafe operation so avoid use.
   */
  public void killAllThreads() {
    if (currentProgressReporter != null && currentProgressReporter.isAlive()) {
      currentProgressReporter.stop();
    }
  }

  public void pleaseStopRunning() {
    shouldRun = false;
  }

  public abstract void start(long startKey, long endKey, int numThreads);
}
