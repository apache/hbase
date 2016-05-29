/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A MultiThreadReader that helps to work with ACL
 */
public class MultiThreadedReaderWithACL extends MultiThreadedReader {
  private static final Log LOG = LogFactory.getLog(MultiThreadedReaderWithACL.class);

  private static final String COMMA = ",";
  /**
   * Maps user with Table instance. Because the table instance has to be created
   * per user inorder to work in that user's context
   */
  private Map<String, Table> userVsTable = new HashMap<String, Table>();
  private Map<String, User> users = new HashMap<String, User>();
  private String[] userNames;

  public MultiThreadedReaderWithACL(LoadTestDataGenerator dataGen, Configuration conf,
      TableName tableName, double verifyPercent, String userNames) throws IOException {
    super(dataGen, conf, tableName, verifyPercent);
    this.userNames = userNames.split(COMMA);
  }

  @Override
  protected void addReaderThreads(int numThreads) throws IOException {
    for (int i = 0; i < numThreads; ++i) {
      HBaseReaderThread reader = new HBaseReaderThreadWithACL(i);
      readers.add(reader);
    }
  }

  public class HBaseReaderThreadWithACL extends HBaseReaderThread {

    public HBaseReaderThreadWithACL(int readerId) throws IOException {
      super(readerId);
    }

    @Override
    protected Table createTable() throws IOException {
      return null;
    }

    @Override
    protected void closeTable() {
      for (Table table : userVsTable.values()) {
        try {
          table.close();
        } catch (Exception e) {
          LOG.error("Error while closing the table " + table.getName(), e);
        }
      }
    }

    @Override
    public void queryKey(final Get get, final boolean verify, final long keyToRead)
        throws IOException {
      final String rowKey = Bytes.toString(get.getRow());

      // read the data
      final long start = System.nanoTime();
      PrivilegedExceptionAction<Object> action = new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          Table localTable = null;
          try {
            Result result = null;
            int specialPermCellInsertionFactor = Integer.parseInt(dataGenerator.getArgs()[2]);
            int mod = ((int) keyToRead % userNames.length);
            if (userVsTable.get(userNames[mod]) == null) {
              localTable = connection.getTable(tableName);
              userVsTable.put(userNames[mod], localTable);
              result = localTable.get(get);
            } else {
              localTable = userVsTable.get(userNames[mod]);
              result = localTable.get(get);
            }
            boolean isNullExpected = ((((int) keyToRead % specialPermCellInsertionFactor)) == 0);
            long end = System.nanoTime();
            verifyResultsAndUpdateMetrics(verify, get, end - start, result, localTable, isNullExpected);
          } catch (IOException e) {
            recordFailure(keyToRead);
          }
          return null;
        }
      };
      if (userNames != null && userNames.length > 0) {
        int mod = ((int) keyToRead % userNames.length);
        User user;
        UserGroupInformation realUserUgi;
        if(!users.containsKey(userNames[mod])) {
          if(User.isHBaseSecurityEnabled(conf)) {
            realUserUgi = LoadTestTool.loginAndReturnUGI(conf, userNames[mod]);
          } else {
            realUserUgi = UserGroupInformation.createRemoteUser(userNames[mod]);
          }
          user = User.create(realUserUgi);
          users.put(userNames[mod], user);
        } else {
          user = users.get(userNames[mod]);
        }
        try {
          user.runAs(action);
        } catch (Exception e) {
          recordFailure(keyToRead);
        }
      }
    }

    private void recordFailure(final long keyToRead) {
      numReadFailures.addAndGet(1);
      LOG.debug("[" + readerId + "] FAILED read, key = " + (keyToRead + "") + ", "
          + "time from start: " + (System.currentTimeMillis() - startTimeMs) + " ms");
    }
  }

}
