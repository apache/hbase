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
package org.apache.hadoop.hbase.ipc;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

/**
 * The result structure of scanOpen and scanNext.
 *
 * ScannerResult is an immutable object, use ScannerResult.Builder to create
 * one.
 */
@ThriftStruct(builder = ScannerResult.Builder.class)
public class ScannerResult {
  private final boolean eos;
  private final boolean eor;
  private final List<Result> results;
  private final long id;

  private ScannerResult(boolean eos, boolean eor, List<Result> results, long id) {
    this.eos = eos;
    this.eor = eor;
    this.results = results;
    this.id = id;
  }

  /**
   * @return End-Of-Scan mark. If true, the whole scanning is over, no need to
   *         call next region, the scanner has been closed.
   */
  @ThriftField(1)
  public boolean getEOS() {
    return eos;
  }

  /**
   * @return End-Of-Region mark. If true, the scanning on this region has been
   *         over, the scanner has been closed.
   */
  @ThriftField(2)
  public boolean getEOR() {
    return eor;
  }

  /**
   * @return  the Result array. Never null.
   */
  @ThriftField(3)
  public List<Result> getResults() {
    return results;
  }

  /**
   * @return  the ID of the scanner.
   */
  @ThriftField(4)
  public long getID() {
    return id;
  }

  /**
   * Builder of ScannerResult.
   */
  public static class Builder {
    private static final List<Result> EMPTY_RESULTS = Collections.emptyList();

    private boolean eos = false;
    private boolean eor = false;
    private List<Result> results = EMPTY_RESULTS;
    private long id = -1L;

    @ThriftField(value = 1, name = "EOS")
    public Builder setEOS(boolean vl) {
      this.eos = vl;
      return this;
    }

    @ThriftField(value = 2, name = "EOR")
    public Builder setEOR(boolean vl) {
      this.eor = vl;
      return this;
    }

    public Builder setResult(Result[] vl) {
      if (vl == null || vl.length == 0) {
        this.results = EMPTY_RESULTS;
      } else {
        this.results = Arrays.asList(vl);
      }
      return this;
    }

    @ThriftField(3)
    public Builder setResult(List<Result> vl) {
      this.results = vl;
      return this;
    }

    @ThriftField(value = 4, name = "ID")
    public Builder setID(long vl) {
      this.id = vl;
      return this;
    }

    public boolean getEOS() {
      return eos;
    }

    public boolean getEOR() {
      return eor;
    }

    public List<Result> getResults() {
      return results;
    }

    public long getID() {
      return id;
    }

    /**
     * Creates a ScannerResult with the current status.
     */
    @ThriftConstructor
    public ScannerResult build() {
      return new ScannerResult(eos, eor, results, id);
    }
  }
}
