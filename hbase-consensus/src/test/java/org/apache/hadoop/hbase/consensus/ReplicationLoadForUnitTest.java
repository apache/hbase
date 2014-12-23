package org.apache.hadoop.hbase.consensus;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ReplicationLoadForUnitTest {

  private volatile int transactionNums = 0;
  private ThreadPoolExecutor loadGeneratorExecutor;
  private volatile boolean stop = false;

  private RaftTestUtil util;
  private QuorumInfo quorumInfo;
  private QuorumClient client;
  private int quorumSize = 5;
  private int majoritySize = 3;

  private volatile long sleepTime = 50;

  public ReplicationLoadForUnitTest(QuorumInfo quorumInfo, QuorumClient client,
                                    RaftTestUtil util, int quorumSize, int majoritySize) {
    this.quorumInfo = quorumInfo;
    this.client = client;
    this.util = util;
    this.quorumSize = quorumSize;
    this.majoritySize = majoritySize;
  }


  public int makeProgress(long sleepTime, int prevLoad) throws InterruptedException {
    System.out.println("Let the client load fly for " + sleepTime + " ms");
    Thread.sleep(sleepTime);
    util.printStatusOfQuorum(quorumInfo);

    while (transactionNums <= prevLoad) {
      System.out.println("No Progress ! prev " + prevLoad + " current " + transactionNums);
      util.printStatusOfQuorum(quorumInfo);
      Thread.sleep(sleepTime);
    }

    return transactionNums;
  }

  public void startReplicationLoad(final int progressInterval) {
    loadGeneratorExecutor = new ThreadPoolExecutor(1, 1,
      0L, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue<Runnable>());

    loadGeneratorExecutor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          while (!stop) {
            try {
              client.replicateCommits(RaftTestUtil.generateTransaction(1 * 1024));
              if ((++transactionNums) % progressInterval == 0) {
                System.out.println("Sent " + transactionNums + " transactions to the quorum");
                util.printStatusOfQuorum(quorumInfo);
              }

            } catch (Exception e) {
              System.out.println("Failed to replicate transactions due to " + e);
            }

            Thread.sleep(sleepTime);
          }
        } catch (InterruptedException e) {
          System.out.println("Failed to replicate transactions due to  " + e);
        }
      }
    });
  }

  public void stopReplicationLoad() throws InterruptedException {
    stop = true;
    loadGeneratorExecutor.shutdownNow();
    loadGeneratorExecutor.awaitTermination(10, TimeUnit.SECONDS);
    System.out.println("Shutdown the replication load and " + transactionNums + " transactions " +
      "have been successfully replicated");
  }

  public void slowDownReplicationLoad() throws InterruptedException {
    sleepTime = 200;
  }

}
