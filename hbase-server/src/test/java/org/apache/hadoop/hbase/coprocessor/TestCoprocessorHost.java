/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestCoprocessorHost {
  /**
   * An {@link Abortable} implementation for tests.
   */
  class TestAbortable implements Abortable {
    private volatile boolean aborted = false;

    @Override
    public void abort(String why, Throwable e) {
      this.aborted = true;
      Assert.fail();
    }

    @Override
    public boolean isAborted() {
      return this.aborted;
    }
  }

  @Test
  public void testDoubleLoading() {
    final Configuration conf = HBaseConfiguration.create();
    CoprocessorHost<CoprocessorEnvironment> host =
        new CoprocessorHost<CoprocessorEnvironment>(new TestAbortable()) {
      final Configuration cpHostConf = conf;

      @Override
      public CoprocessorEnvironment createEnvironment(Class<?> implClass,
          final Coprocessor instance, int priority, int sequence, Configuration conf) {
        return new CoprocessorEnvironment() {
          final Coprocessor envInstance = instance;

          @Override
          public int getVersion() {
            return 0;
          }

          @Override
          public String getHBaseVersion() {
            return "0.0.0";
          }

          @Override
          public Coprocessor getInstance() {
            return envInstance;
          }

          @Override
          public int getPriority() {
            return 0;
          }

          @Override
          public int getLoadSequence() {
            return 0;
          }

          @Override
          public Configuration getConfiguration() {
            return cpHostConf;
          }

          @Override
          public Table getTable(TableName tableName) throws IOException {
            return null;
          }

          @Override
          public Table getTable(TableName tableName, ExecutorService service) throws IOException {
            return null;
          }

          @Override
          public ClassLoader getClassLoader() {
            return null;
          }
        };
      }
    };
    final String key = "KEY";
    final String coprocessor = "org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver";
    // Try and load coprocessor three times.
    conf.setStrings(key, coprocessor, coprocessor, coprocessor);
    host.loadSystemCoprocessors(conf, key);
    // Only one coprocessor loaded
    Assert.assertEquals(1, host.coprocessors.size());
  }
}