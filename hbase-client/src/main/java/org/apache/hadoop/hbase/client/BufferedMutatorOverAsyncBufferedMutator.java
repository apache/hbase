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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link BufferedMutator} implementation based on {@link AsyncBufferedMutator}.
 */
@InterfaceAudience.Private
class BufferedMutatorOverAsyncBufferedMutator implements BufferedMutator {

  private static final Logger LOG =
    LoggerFactory.getLogger(BufferedMutatorOverAsyncBufferedMutator.class);

  private final AsyncBufferedMutator mutator;

  private final ExceptionListener listener;

  private final Set<CompletableFuture<Void>> futures = ConcurrentHashMap.newKeySet();

  private final AtomicLong bufferedSize = new AtomicLong(0);

  private final ConcurrentLinkedQueue<Pair<Mutation, Throwable>> errors =
    new ConcurrentLinkedQueue<>();

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

  private void internalFlush() throws RetriesExhaustedWithDetailsException {
    // should get the future array before calling mutator.flush, otherwise we may hit an infinite
    // wait, since someone may add new future to the map after we calling the flush.
    CompletableFuture<?>[] toWait = futures.toArray(new CompletableFuture<?>[0]);
    mutator.flush();
    try {
      CompletableFuture.allOf(toWait).join();
    } catch (CompletionException e) {
      // just ignore, we will record the actual error in the errors field
      LOG.debug("Flush failed, you should get an exception thrown to your code", e);
    }
    if (!errors.isEmpty()) {
      RetriesExhaustedWithDetailsException error = makeError();
      listener.onException(error, this);
    }
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws IOException {
    List<CompletableFuture<Void>> fs = mutator.mutate(mutations);
    for (int i = 0, n = fs.size(); i < n; i++) {
      CompletableFuture<Void> toComplete = new CompletableFuture<>();
      futures.add(toComplete);
      Mutation mutation = mutations.get(i);
      long heapSize = mutation.heapSize();
      bufferedSize.addAndGet(heapSize);
      addListener(fs.get(i), (r, e) -> {
        futures.remove(toComplete);
        bufferedSize.addAndGet(-heapSize);
        if (e != null) {
          errors.add(Pair.newPair(mutation, e));
          toComplete.completeExceptionally(e);
        } else {
          toComplete.complete(r);
        }
      });
    }
    synchronized (this) {
      if (bufferedSize.get() > mutator.getWriteBufferSize() * 2) {
        // We have too many mutations which are not completed yet, let's call a flush to release the
        // memory to prevent OOM
        // We use buffer size * 2 is because that, the async buffered mutator will flush
        // automatically when the write buffer size limit is reached, so usually we do not need to
        // call flush explicitly if the buffered size is only a little larger than the buffer size
        // limit. But if the buffered size is too large(2 times of the buffer size), we still need
        // to block here to prevent OOM.
        internalFlush();
      } else if (!errors.isEmpty()) {
        RetriesExhaustedWithDetailsException error = makeError();
        listener.onException(error, this);
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    internalFlush();
    mutator.close();
  }

  @Override
  public synchronized void flush() throws IOException {
    internalFlush();
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

  @Override
  public Map<String, byte[]> getRequestAttributes() {
    return mutator.getRequestAttributes();
  }
}
