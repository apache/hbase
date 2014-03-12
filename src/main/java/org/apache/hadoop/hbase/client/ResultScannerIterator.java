/**
 * Copyright 2014 The Apache Software Foundation
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

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An Iterator of Result for ResultScanner.
 */
public class ResultScannerIterator implements Iterator<Result> {
  private ResultScanner scanner;
  private Result next = null;

  public ResultScannerIterator(ResultScanner scanner) {
    this.scanner = scanner;
  }

  @Override
  public boolean hasNext() {
    if (next != null) {
      return true;
    }

    try {
      next = scanner.next();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return next != null;
  }

  @Override
  public Result next() {
    if (!hasNext()) {
      throw new NoSuchElementException("ResultScannerIterator");
    }
    Result res = next;
    next = null;
    return res;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }
}
