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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Objects;

/**
 * The thrift serializable version of RowMutations class.
 * Contains 2 seperate lists to hold Puts and Deletes.
 *
 */
@ThriftStruct
public class TRowMutations extends RowMutations {
  private final byte[] row;
  /**
   * Both {@link #puts} and {@link #deletes} contain equal number of elements.
   */
  private final List<Put> puts;
  private final List<Delete> deletes;


  @SuppressWarnings("deprecation")
  @ThriftConstructor
  public TRowMutations(@ThriftField(1) byte[] row,
      @ThriftField(2) List<Put> puts,
      @ThriftField(3) List<Delete> deletes) {
    this.row = row;
    this.puts = puts;
    this.deletes = deletes;
  }

  @ThriftField(1)
  public byte[] getRow() {
    return this.row;
  }

  @ThriftField(2)
  public List<Put> getPuts() {
    return this.puts;
  }

  @ThriftField(3)
  public List<Delete> getDeletes() {
    return this.deletes;
  }

  /**
   * The equals method is not used anywhere other than unit tests.
   * We can make this more efficient(nlogn) if we need it in production code.
   */
  @Override
  public boolean equals(Object obj) {
    TRowMutations objCast;
    if (obj instanceof TRowMutations) {
      objCast = (TRowMutations)obj;
    } else {
      return false;
    }
    if (Bytes.compareTo(this.row, objCast.row) != 0) {
      return false;
    }
    if (this.getPuts() == null && objCast.getDeletes() == null) {
      return true;
    }
    if (this.getPuts() == null ^ objCast.getDeletes() == null) {
      return false;
    }
    if (!this.getPuts().containsAll(objCast.getPuts())) {
      return false;
    }
    if (!objCast.getPuts().containsAll(this.getPuts())) {
      return false;
    }
    if (!this.getDeletes().containsAll(objCast.getDeletes())) {
      return false;
    }
    if (!objCast.getDeletes().containsAll(this.getDeletes())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(row, puts, deletes);
  }

  /**
   * This class is not thread safe. Once the object is built by this builder,
   * it can be used in a thread safe manner.
   */
  public static class Builder {
    private final byte[] row;
    private List<Put> puts = new ArrayList<Put>();
    private List<Delete> deletes = new ArrayList<Delete>();

    public Builder(byte[] row) {
      this.row = row;
    }

    public Builder addPut(Put p) throws IOException {
      RowMutations.checkRow(this.row, p.getRow());
      this.puts.add(p);
      this.deletes.add(Delete.createDummyDelete());
      return this;
    }

    public Builder addDelete(Delete d) throws IOException {
      RowMutations.checkRow(this.row, d.getRow());
      this.deletes.add(d);
      this.puts.add(Put.createDummyPut());
      return this;
    }

    public TRowMutations create() {
      return new TRowMutations(this.row, this.puts, this.deletes);
    }

    public static TRowMutations createFromRowMutations(RowMutations arm)
        throws IOException {
      TRowMutations.Builder builder = new TRowMutations.Builder(arm.getRow());
      for (Mutation m : arm.getMutations()) {
        if (m instanceof Put) {
          builder.addPut((Put)m);
        } else if (m instanceof Delete) {
          builder.addDelete((Delete)m);
        }
      }
      return builder.create();
    }
  }
}
