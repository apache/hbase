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
package org.apache.hadoop.hbase.mapreduce.replication;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class VerifyReplicationRecompareRunnable implements Runnable {

  private static final Logger LOG =
    LoggerFactory.getLogger(VerifyReplicationRecompareRunnable.class);

  private final Mapper<?, ?, ?, ?>.Context context;
  private final VerifyReplication.Verifier.Counters originalCounter;
  private final String delimiter;
  private final byte[] row;
  private final Scan tableScan;
  private final Table sourceTable;
  private final Table replicatedTable;

  private final int reCompareTries;
  private final int sleepMsBeforeReCompare;
  private final int reCompareBackoffExponent;
  private final boolean verbose;

  private Result sourceResult;
  private Result replicatedResult;

  public VerifyReplicationRecompareRunnable(Mapper<?, ?, ?, ?>.Context context, Result sourceResult,
    Result replicatedResult, VerifyReplication.Verifier.Counters originalCounter, String delimiter,
    Scan tableScan, Table sourceTable, Table replicatedTable, int reCompareTries,
    int sleepMsBeforeReCompare, int reCompareBackoffExponent, boolean verbose) {
    this.context = context;
    this.sourceResult = sourceResult;
    this.replicatedResult = replicatedResult;
    this.originalCounter = originalCounter;
    this.delimiter = delimiter;
    this.tableScan = tableScan;
    this.sourceTable = sourceTable;
    this.replicatedTable = replicatedTable;
    this.reCompareTries = reCompareTries;
    this.sleepMsBeforeReCompare = sleepMsBeforeReCompare;
    this.reCompareBackoffExponent = reCompareBackoffExponent;
    this.verbose = verbose;
    this.row = VerifyReplication.getRow(sourceResult, replicatedResult);
  }

  @Override
  public void run() {
    Get get = new Get(row);
    get.setCacheBlocks(tableScan.getCacheBlocks());
    get.setFilter(tableScan.getFilter());

    int sleepMs = sleepMsBeforeReCompare;
    int tries = 0;

    while (++tries <= reCompareTries) {
      context.getCounter(VerifyReplication.Verifier.Counters.RECOMPARES).increment(1);

      try {
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        LOG.warn("Sleeping interrupted, incrementing bad rows and aborting");
        incrementOriginalAndBadCounter();
        context.getCounter(VerifyReplication.Verifier.Counters.FAILED_RECOMPARE).increment(1);
        Thread.currentThread().interrupt();
        return;
      }

      try {
        if (fetchLatestRows(get) && matches(sourceResult, replicatedResult, null)) {
          if (verbose) {
            LOG.info("Good row key (with recompare): {}{}{}", delimiter, Bytes.toStringBinary(row),
              delimiter);
          }
          context.getCounter(VerifyReplication.Verifier.Counters.GOODROWS).increment(1);
          return;
        } else {
          context.getCounter(VerifyReplication.Verifier.Counters.FAILED_RECOMPARE).increment(1);
        }
      } catch (IOException e) {
        context.getCounter(VerifyReplication.Verifier.Counters.FAILED_RECOMPARE).increment(1);
        if (verbose) {
          LOG.info("Got an exception during recompare for rowkey={}", Bytes.toStringBinary(row), e);
        }
      }

      sleepMs = sleepMs * (2 ^ reCompareBackoffExponent);
    }

    LOG.error("{}, rowkey={}{}{}", originalCounter, delimiter, Bytes.toStringBinary(row),
      delimiter);
    incrementOriginalAndBadCounter();
  }

  public void fail() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Called fail on row={}", Bytes.toStringBinary(row));
    }
    incrementOriginalAndBadCounter();
    context.getCounter(VerifyReplication.Verifier.Counters.FAILED_RECOMPARE).increment(1);
  }

  private boolean fetchLatestRows(Get get) throws IOException {
    Result sourceResult = sourceTable.get(get);
    Result replicatedResult = replicatedTable.get(get);

    boolean sourceMatches = matches(sourceResult, this.sourceResult,
      VerifyReplication.Verifier.Counters.SOURCE_ROW_CHANGED);
    boolean replicatedMatches = matches(replicatedResult, this.replicatedResult,
      VerifyReplication.Verifier.Counters.PEER_ROW_CHANGED);

    this.sourceResult = sourceResult;
    this.replicatedResult = replicatedResult;
    return sourceMatches && replicatedMatches;
  }

  private boolean matches(Result original, Result updated,
    VerifyReplication.Verifier.Counters failCounter) {
    try {
      Result.compareResults(original, updated);
      return true;
    } catch (Exception e) {
      if (failCounter != null) {
        context.getCounter(failCounter).increment(1);
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} for rowkey={}", failCounter, Bytes.toStringBinary(row));
        }
      }
      return false;
    }
  }

  private void incrementOriginalAndBadCounter() {
    context.getCounter(originalCounter).increment(1);
    context.getCounter(VerifyReplication.Verifier.Counters.BADROWS).increment(1);
  }
}
