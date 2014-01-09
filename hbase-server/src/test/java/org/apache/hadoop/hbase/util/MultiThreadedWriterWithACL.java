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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.apache.hadoop.util.StringUtils;

/**
 * MultiThreadedWriter that helps in testing ACL
 */
public class MultiThreadedWriterWithACL extends MultiThreadedWriter {

  private static final Log LOG = LogFactory.getLog(MultiThreadedWriterWithACL.class);
  private User userOwner;

  public MultiThreadedWriterWithACL(LoadTestDataGenerator dataGen, Configuration conf,
      TableName tableName, User userOwner) {
    super(dataGen, conf, tableName);
    this.userOwner = userOwner;
  }

  @Override
  public void start(long startKey, long endKey, int numThreads) throws IOException {
    super.start(startKey, endKey, numThreads);
  }

  @Override
  protected void createWriterThreads(int numThreads) throws IOException {
    for (int i = 0; i < numThreads; ++i) {
      HBaseWriterThread writer = new HBaseWriterThreadWithACL(i);
      writers.add(writer);
    }
  }

  public class HBaseWriterThreadWithACL extends HBaseWriterThread {

    private HTable table;
    private WriteAccessAction writerAction = new WriteAccessAction();

    public HBaseWriterThreadWithACL(int writerId) throws IOException {
      super(writerId);
    }

    @Override
    protected HTable createTable() throws IOException {
      return null;
    }

    @Override
    protected void closeHTable() {
      if (table != null) {
        try {
          table.close();
        } catch (Exception e) {
          LOG.error("Error in closing the table "+table.getName(), e);
        }
      }
    }

    @Override
    public void insert(final HTable table, Put put, final long keyBase) {
      final long start = System.currentTimeMillis();
      try {
        put = (Put) dataGenerator.beforeMutate(keyBase, put);
        writerAction.setPut(put);
        writerAction.setKeyBase(keyBase);
        writerAction.setStartTime(start);
        userOwner.runAs(writerAction);
      } catch (IOException e) {
        recordFailure(table, put, keyBase, start, e);
      } catch (InterruptedException e) {
        failedKeySet.add(keyBase);
      }
    }

    class WriteAccessAction implements PrivilegedExceptionAction<Object> {
      private Put put;
      private long keyBase;
      private long start;

      public WriteAccessAction() {
      }

      public void setPut(final Put put) {
        this.put = put;
      }

      public void setKeyBase(final long keyBase) {
        this.keyBase = keyBase;
      }

      public void setStartTime(final long start) {
        this.start = start;
      }

      @Override
      public Object run() throws Exception {
        try {
          if (table == null) {
            table = new HTable(conf, tableName);
          }
          table.put(put);
        } catch (IOException e) {
          recordFailure(table, put, keyBase, start, e);
        }
        return null;
      }
    }
  }

  private void recordFailure(final HTable table, final Put put, final long keyBase,
      final long start, IOException e) {
    failedKeySet.add(keyBase);
    String exceptionInfo;
    if (e instanceof RetriesExhaustedWithDetailsException) {
      RetriesExhaustedWithDetailsException aggEx = (RetriesExhaustedWithDetailsException) e;
      exceptionInfo = aggEx.getExhaustiveDescription();
    } else {
      StringWriter stackWriter = new StringWriter();
      PrintWriter pw = new PrintWriter(stackWriter);
      e.printStackTrace(pw);
      pw.flush();
      exceptionInfo = StringUtils.stringifyException(e);
    }
    LOG.error("Failed to insert: " + keyBase + " after " + (System.currentTimeMillis() - start)
        + "ms; region information: " + getRegionDebugInfoSafe(table, put.getRow()) + "; errors: "
        + exceptionInfo);
  }
}
