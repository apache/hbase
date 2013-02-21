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
package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.io.Serializable;

/**
 * Wrapper class which returns the result of the bulk deletion operation happened at the server for
 * a region. This includes the total number of rows deleted and/or any {@link IOException} which is
 * happened while doing the operation. It will also include total number of versions deleted, when
 * the delete type is VERSION.
 */
public class BulkDeleteResponse implements Serializable {
  private static final long serialVersionUID = -8192337710525997237L;
  private long rowsDeleted;
  private IOException ioException;
  private long versionsDeleted;

  public BulkDeleteResponse() {

  }

  public void setRowsDeleted(long rowsDeleted) {
    this.rowsDeleted = rowsDeleted;
  }

  public long getRowsDeleted() {
    return rowsDeleted;
  }

  public void setIoException(IOException ioException) {
    this.ioException = ioException;
  }

  public IOException getIoException() {
    return ioException;
  }

  public long getVersionsDeleted() {
    return versionsDeleted;
  }

  public void setVersionsDeleted(long versionsDeleted) {
    this.versionsDeleted = versionsDeleted;
  }
}