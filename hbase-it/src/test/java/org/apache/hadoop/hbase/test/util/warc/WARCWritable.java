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
package org.apache.hadoop.hbase.test.util.warc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * A mutable wrapper around a {@link WARCRecord} implementing the Hadoop Writable interface. This
 * allows WARC records to be used throughout Hadoop (e.g. written to sequence files when shuffling
 * data between mappers and reducers). The record is encoded as a single record in standard WARC/1.0
 * format.
 */
public class WARCWritable implements Writable {

  private WARCRecord record;

  /** Creates an empty writable (with a null record). */
  public WARCWritable() {
    this.record = null;
  }

  /** Creates a writable wrapper around a given WARCRecord. */
  public WARCWritable(WARCRecord record) {
    this.record = record;
  }

  /** Returns the record currently wrapped by this writable. */
  public WARCRecord getRecord() {
    return record;
  }

  /** Updates the record held within this writable wrapper. */
  public void setRecord(WARCRecord record) {
    this.record = record;
  }

  /** Appends the current record to a {@link DataOutput} stream. */
  @Override
  public void write(DataOutput out) throws IOException {
    if (record != null) {
      record.write(out);
    }
  }

  /**
   * Parses a {@link WARCRecord} out of a {@link DataInput} stream, and makes it the writable's
   * current record.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    record = new WARCRecord(in);
  }

}
