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
package org.apache.hadoop.hbase.io.devsim;

/**
 * Token-bucket rate limiter. Supports a configurable tokens-per-second rate with windowed refill.
 * Used for per-volume BW budgets, per-volume IOPS budgets, and instance-level aggregate BW caps.
 * <p>
 * When all tokens in the current window are consumed, {@link #consume(long)} blocks until the next
 * window opens, providing backpressure to the caller.
 */
public class IOBudget {

  private final long tokensPerSec;
  private final long tokensPerWindow;
  private final long windowMs;
  private final long millisPerToken;
  private final boolean lowRateMode;
  private long availableTokens;
  private long windowStartTime;
  private long nextTokenTimeMs;

  /**
   * @param tokensPerSec tokens replenished per second (e.g. bytes/sec for BW, ops/sec for IOPS)
   * @param windowMs     refill window duration in milliseconds
   */
  public IOBudget(long tokensPerSec, long windowMs) {
    this.tokensPerSec = tokensPerSec;
    this.windowMs = windowMs;
    this.tokensPerWindow = tokensPerSec * windowMs / 1000;
    this.lowRateMode = tokensPerSec > 0 && this.tokensPerWindow == 0;
    this.millisPerToken = lowRateMode ? Math.max(1, (1000 + tokensPerSec - 1) / tokensPerSec) : 0;
    this.availableTokens = lowRateMode ? 0 : tokensPerWindow;
    this.windowStartTime = System.currentTimeMillis();
    this.nextTokenTimeMs = this.windowStartTime;
  }

  public synchronized void reset() {
    this.availableTokens = lowRateMode ? 0 : tokensPerWindow;
    this.windowStartTime = System.currentTimeMillis();
    this.nextTokenTimeMs = this.windowStartTime;
  }

  /**
   * Consume tokens from the budget. Blocks (sleeps) if the budget is exhausted until enough tokens
   * are available.
   * @param tokens number of tokens to consume
   * @return total milliseconds slept waiting for tokens
   */
  public synchronized long consume(long tokens) {
    if (tokens <= 0) {
      return 0;
    }
    if (tokensPerSec <= 0) {
      return 0;
    }
    if (lowRateMode) {
      return consumeLowRate(tokens);
    }
    long totalSlept = 0;
    long remaining = tokens;
    while (remaining > 0) {
      long now = System.currentTimeMillis();
      long elapsed = now - windowStartTime;
      if (elapsed >= windowMs) {
        long windowsPassed = elapsed / windowMs;
        availableTokens = tokensPerWindow;
        windowStartTime += windowsPassed * windowMs;
      }
      long toConsume = Math.min(remaining, availableTokens);
      if (toConsume > 0) {
        availableTokens -= toConsume;
        remaining -= toConsume;
      }
      if (remaining > 0) {
        long sleepTime = windowMs - (System.currentTimeMillis() - windowStartTime);
        if (sleepTime <= 0) {
          sleepTime = 1;
        }
        totalSlept += sleepTime;
        try {
          wait(sleepTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    notifyAll();
    return totalSlept;
  }

  /**
   * Low-rate path for configurations where {@code tokensPerWindow == 0}. In this case we schedule
   * one token every {@code millisPerToken} and block callers until the next token time.
   */
  private long consumeLowRate(long tokens) {
    long totalSlept = 0;
    for (long i = 0; i < tokens; i++) {
      long now = System.currentTimeMillis();
      if (now < nextTokenTimeMs) {
        long sleepTime = nextTokenTimeMs - now;
        totalSlept += sleepTime;
        try {
          wait(sleepTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      } else {
        nextTokenTimeMs = now;
      }
      nextTokenTimeMs += millisPerToken;
    }
    notifyAll();
    return totalSlept;
  }
}
