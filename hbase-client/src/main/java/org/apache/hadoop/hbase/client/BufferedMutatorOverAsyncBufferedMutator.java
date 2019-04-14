/**
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

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * {@link BufferedMutator} implementation based on {@link AsyncBufferedMutator}.
 */
@InterfaceAudience.Private
class BufferedMutatorOverAsyncBufferedMutator implements BufferedMutator {

  private final AsyncBufferedMutator mutator;

  private final ExceptionListener listener;

  private List<CompletableFuture<Void>> futures = new ArrayList<>();

  private final ConcurrentLinkedQueue<Pair<Mutation, Throwable>> errors =
    new ConcurrentLinkedQueue<>();

  private final static int BUFFERED_FUTURES_THRESHOLD = 1024;

  BufferedMutatorOverAsyncBufferedMutator(AsyncBufferedMutator mutator,
      ExceptionListener listener) {
    this.mutator = mutator;
    this.listener = listener;
  }

  @Override
  public TableName getName() {
    return mutator.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return mutator.getConfiguration();
  }

  @Override
  public void mutate(Mutation mutation) throws IOException {
    mutate(Collections.singletonList(mutation));
  }

  private static final Pattern ADDR_MSG_MATCHER = Pattern.compile("Call to (\\S+) failed");

  // not always work, so may return an empty string
  private String getHostnameAndPort(Throwable error) {
    Matcher matcher = ADDR_MSG_MATCHER.matcher(error.getMessage());
    if (matcher.matches()) {
      return matcher.group(1);
    } else {
      return "";
    }
  }

  private RetriesExhaustedWithDetailsException makeError() {
    List<Row> rows = new ArrayList<>();
    List<Throwable> throwables = new ArrayList<>();
    List<String> hostnameAndPorts = new ArrayList<>();
    for (;;) {
      Pair<Mutation, Throwable> pair = errors.poll();
      if (pair == null) {
        break;
      }
      rows.add(pair.getFirst());
      throwables.add(pair.getSecond());
      hostnameAndPorts.add(getHostnameAndPort(pair.getSecond()));
    }
    return new RetriesExhaustedWithDetailsException(throwables, rows, hostnameAndPorts);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws IOException {
    List<CompletableFuture<Void>> toBuffered = new ArrayList<>();
    List<CompletableFuture<Void>> fs = mutator.mutate(mutations);
    for (int i = 0, n = fs.size(); i < n; i++) {
      CompletableFuture<Void> toComplete = new CompletableFuture<>();
      final int index = i;
      addListener(fs.get(index), (r, e) -> {
        if (e != null) {
          errors.add(Pair.newPair(mutations.get(index), e));
          toComplete.completeExceptionally(e);
        } else {
          toComplete.complete(r);
        }
      });
      toBuffered.add(toComplete);
    }
    synchronized (this) {
      futures.addAll(toBuffered);
      if (futures.size() > BUFFERED_FUTURES_THRESHOLD) {
        tryCompleteFuture();
      }
      if (!errors.isEmpty()) {
        RetriesExhaustedWithDetailsException error = makeError();
        listener.onException(error, this);
      }
    }
  }

  private void tryCompleteFuture() {
    futures = futures.stream().filter(f -> !f.isDone()).collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {
    flush();
    mutator.close();
  }

  @Override
  public void flush() throws IOException {
    mutator.flush();
    synchronized (this) {
      List<CompletableFuture<Void>> toComplete = this.futures;
      this.futures = new ArrayList<>();
      try {
        CompletableFuture.allOf(toComplete.toArray(new CompletableFuture<?>[toComplete.size()]))
          .join();
      } catch (CompletionException e) {
        // just ignore, we will record the actual error in the errors field
      }
      if (!errors.isEmpty()) {
        RetriesExhaustedWithDetailsException error = makeError();
        listener.onException(error, this);
      }
    }
  }

  @Override
  public long getWriteBufferSize() {
    return mutator.getWriteBufferSize();
  }

  @Override
  public void setRpcTimeout(int timeout) {
    // no effect
  }

  @Override
  public void setOperationTimeout(int timeout) {
    // no effect
  }
}
