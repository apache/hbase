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
package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Interface for client-side scanning. Go to {@link Table} to obtain instances.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ResultScanner extends Closeable, Iterable<Result> {

  @Override
  default Iterator<Result> iterator() {
    return new Iterator<Result>() {
      // The next RowResult, possibly pre-read
      Result next = null;

      // return true if there is another item pending, false if there isn't.
      // this method is where the actual advancing takes place, but you need
      // to call next() to consume it. hasNext() will only advance if there
      // isn't a pending next().
      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        }
        try {
          return (next = ResultScanner.this.next()) != null;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      // get the pending next item and advance the iterator. returns null if
      // there is no next item.
      @Override
      public Result next() {
        // since hasNext() does the real advancing, we call this to determine
        // if there is a next before proceeding.
        if (!hasNext()) {
          return null;
        }

        // if we get to here, then hasNext() has given us an item to return.
        // we want to return the item and then null out the next pointer, so
        // we use a temporary variable.
        Result temp = next;
        next = null;
        return temp;
      }
    };
  }

  /**
   * Grab the next row's worth of values. The scanner will return a Result.
   * @return Result object if there is another row, null if the scanner is exhausted.
   * @throws IOException e
   */
  Result next() throws IOException;

  /**
   * Get nbRows rows. How many RPCs are made is determined by the {@link Scan#setCaching(int)}
   * setting (or hbase.client.scanner.caching in hbase-site.xml).
   * @param nbRows number of rows to return
   * @return Between zero and nbRows rowResults. Scan is done if returned array is of zero-length
   *         (We never return null).
   * @throws IOException
   */
  default Result[] next(int nbRows) throws IOException {
    List<Result> resultSets = new ArrayList<>(nbRows);
    for (int i = 0; i < nbRows; i++) {
      Result next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new Result[0]);
  }

  /**
   * Closes the scanner and releases any resources it has allocated
   */
  @Override
  void close();

  /**
   * Allow the client to renew the scanner's lease on the server.
   * @return true if the lease was successfully renewed, false otherwise.
   */
  boolean renewLease();
}
