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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

/**
 * A MultiThreadUpdater that helps to work with ACL
 */
public class MultiThreadedUpdaterWithACL extends MultiThreadedUpdater {
  private static final Log LOG = LogFactory.getLog(MultiThreadedUpdaterWithACL.class);
  private final static String COMMA= ",";
  private User userOwner;
  /**
   * Maps user with Table instance. Because the table instance has to be created
   * per user inorder to work in that user's context
   */
  private Map<String, HTable> userVsTable = new HashMap<String, HTable>();
  private Map<String, User> users = new HashMap<String, User>();
  private String[] userNames;

  public MultiThreadedUpdaterWithACL(LoadTestDataGenerator dataGen, Configuration conf,
      TableName tableName, double updatePercent, User userOwner, String userNames) {
    super(dataGen, conf, tableName, updatePercent);
    this.userOwner = userOwner;
    this.userNames = userNames.split(COMMA);
  }

  @Override
  protected void addUpdaterThreads(int numThreads) throws IOException {
    for (int i = 0; i < numThreads; ++i) {
      HBaseUpdaterThread updater = new HBaseUpdaterThreadWithACL(i);
      updaters.add(updater);
    }
  }

  public class HBaseUpdaterThreadWithACL extends HBaseUpdaterThread {

    private HTable table;
    private MutateAccessAction mutateAction = new MutateAccessAction();

    public HBaseUpdaterThreadWithACL(int updaterId) throws IOException {
      super(updaterId);
    }

    @Override
    protected HTable createTable() throws IOException {
      return null;
    }

    @Override
    protected void closeHTable() {
      try {
        if (table != null) {
          table.close();
        }
        for (HTable table : userVsTable.values()) {
          try {
            table.close();
          } catch (Exception e) {
            LOG.error("Error while closing the table " + table.getName(), e);
          }
        }
      } catch (Exception e) {
        LOG.error("Error while closing the HTable "+table.getName(), e);
      }
    }

    @Override
    protected Result getRow(final Get get, final long rowKeyBase, final byte[] cf) {
      PrivilegedExceptionAction<Object> action = new PrivilegedExceptionAction<Object>() {

        @Override
        public Object run() throws Exception {
          Result res = null;
          HTable localTable = null;
          try {
            int mod = ((int) rowKeyBase % userNames.length);
            if (userVsTable.get(userNames[mod]) == null) {
              localTable = new HTable(conf, tableName);
              userVsTable.put(userNames[mod], localTable);
              res = localTable.get(get);
            } else {
              localTable = userVsTable.get(userNames[mod]);
              res = localTable.get(get);
            }
          } catch (IOException ie) {
            LOG.warn("Failed to get the row for key = [" + get.getRow() + "], column family = ["
                + Bytes.toString(cf) + "]", ie);
          }
          return res;
        }
      };

      if (userNames != null && userNames.length > 0) {
        int mod = ((int) rowKeyBase % userNames.length);
        User user;
        UserGroupInformation realUserUgi;
        try {
          if (!users.containsKey(userNames[mod])) {
            if (User.isHBaseSecurityEnabled(conf)) {
              realUserUgi = LoadTestTool.loginAndReturnUGI(conf, userNames[mod]);
            } else {
              realUserUgi = UserGroupInformation.createRemoteUser(userNames[mod]);
            }
            user = User.create(realUserUgi);
            users.put(userNames[mod], user);
          } else {
            user = users.get(userNames[mod]);
          }
          Result result = (Result) user.runAs(action);
          return result;
        } catch (Exception ie) {
          LOG.warn("Failed to get the row for key = [" + get.getRow() + "], column family = ["
              + Bytes.toString(cf) + "]", ie);
        }
      }
      // This means that no users were present
      return null;
    }

    @Override
    public void mutate(final HTable table, Mutation m, final long keyBase, final byte[] row,
        final byte[] cf, final byte[] q, final byte[] v) {
      final long start = System.currentTimeMillis();
      try {
        m = dataGenerator.beforeMutate(keyBase, m);
        mutateAction.setMutation(m);
        mutateAction.setCF(cf);
        mutateAction.setRow(row);
        mutateAction.setQualifier(q);
        mutateAction.setValue(v);
        mutateAction.setStartTime(start);
        mutateAction.setKeyBase(keyBase);
        userOwner.runAs(mutateAction);
      } catch (IOException e) {
        recordFailure(m, keyBase, start, e);
      } catch (InterruptedException e) {
        failedKeySet.add(keyBase);
      }
    }

    class MutateAccessAction implements PrivilegedExceptionAction<Object> {
      private HTable table;
      private long start;
      private Mutation m;
      private long keyBase;
      private byte[] row;
      private byte[] cf;
      private byte[] q;
      private byte[] v;

      public MutateAccessAction() {

      }

      public void setStartTime(final long start) {
        this.start = start;
      }

      public void setMutation(final Mutation m) {
        this.m = m;
      }

      public void setRow(final byte[] row) {
        this.row = row;
      }

      public void setCF(final byte[] cf) {
        this.cf = cf;
      }

      public void setQualifier(final byte[] q) {
        this.q = q;
      }

      public void setValue(final byte[] v) {
        this.v = v;
      }

      public void setKeyBase(final long keyBase) {
        this.keyBase = keyBase;
      }

      @Override
      public Object run() throws Exception {
        try {
          if (table == null) {
            table = new HTable(conf, tableName);
          }
          if (m instanceof Increment) {
            table.increment((Increment) m);
          } else if (m instanceof Append) {
            table.append((Append) m);
          } else if (m instanceof Put) {
            table.checkAndPut(row, cf, q, v, (Put) m);
          } else if (m instanceof Delete) {
            table.checkAndDelete(row, cf, q, v, (Delete) m);
          } else {
            throw new IllegalArgumentException("unsupported mutation "
                + m.getClass().getSimpleName());
          }
          totalOpTimeMs.addAndGet(System.currentTimeMillis() - start);
        } catch (IOException e) {
          recordFailure(m, keyBase, start, e);
        }
        return null;
      }
    }

    private void recordFailure(final Mutation m, final long keyBase,
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
      LOG.error("Failed to mutate: " + keyBase + " after " + (System.currentTimeMillis() - start)
          + "ms; region information: " + getRegionDebugInfoSafe(table, m.getRow()) + "; errors: "
          + exceptionInfo);
    }
  }
}
