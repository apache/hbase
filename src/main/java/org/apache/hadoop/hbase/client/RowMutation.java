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
package org.apache.hadoop.hbase.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.WritableComparable;

/**
 * Class to be used in Bulk Import MR jobs to be able to pass both Deletes and Puts in as parameters.
 */
public class RowMutation implements Row {

  /**
   * Enum used for serializing
   */
  public static enum Type {
    Put((byte)4),
    Delete((byte)8);
    
    private final byte code;

    Type(final byte c) {
      this.code = c;
    }

    /**
     * @return the code of this union
     */
    public byte getCode() {
      return this.code;
    }
  }
  
  /**
   * Union type of field to hold either a Put or Delete
   * Useful for abstractions
   */
  private Row row = null;

  /**
   * Global counter for internal ordering of mutations
   */
  private static AtomicLong globalOrderCounter = new AtomicLong(0);

  /**
   * Field to keep track of the internal ordering of mutations
   */
  private long orderNumber;

  /**
   * To be used for Writable. 
   * DO NOT USE!!!
   */
  public RowMutation() {}

  /**
   * Constructor to set the inner union style field.
   * 
   * @param request
   *          the Put or Delete to be executed
   * @throws IOException
   *           if the passed parameter is not of the supported type
   */
  public RowMutation(final WritableComparable<Row> request) throws IOException {
    if (request instanceof Put) {
      row = new Put((Put) request);
    } else if (request instanceof Delete) {
      row = new Delete((Delete) request);
    } else {
      throw new IOException("Type currently not supported: "
          + request.getClass().getName());
    }
  }

  /**
   * Method for getting the Row instance from inside
   * @return row
   */
  public Row getInstance() {
    return row;
  }

  @Override
  public int compareTo(Row o) {
    return row.compareTo(o);
  }

  @Override
  public byte[] getRow() {
    return row.getRow();
  }

  @Override
  public void readFields(DataInput in) 
  throws IOException {
    byte b = in.readByte();
    
    if (Type.Put.getCode() == b) {
      row = new Put();
    } else if (Type.Delete.getCode() == b) {
      row = new Delete();
    } else {
      throw new IOException("Tried to read an invalid type of serialized object!");
    }
    
    this.orderNumber = in.readLong();
    row.readFields(in);
  }

  @Override
  public void write(DataOutput out) 
  throws IOException {
    byte b = 0;
    
    if (row instanceof Put) {
      b = Type.Put.getCode();
    } else if (row instanceof Delete) {
      b = Type.Delete.getCode();
    } else {
      throw new IOException("Tried to write an invalid type of object to serialize!");
    }
    
    out.write(b);
    out.writeLong(RowMutation.globalOrderCounter.incrementAndGet());

    row.write(out);
  }

  /**
   * 
   * @return
   */
  public long getOrderNumber() {
    return this.orderNumber;
  }

}