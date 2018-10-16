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
package org.apache.hadoop.hbase.procedure;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.master.procedure.ProcedureDescriber;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.BytesValue;

@Category({MasterTests.class, SmallTests.class})
public class TestProcedureDescriber {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestProcedureDescriber.class);

  public static class TestProcedure extends Procedure {
    @Override
    protected Procedure[] execute(Object env) throws ProcedureYieldException,
        ProcedureSuspendedException, InterruptedException {
      return null;
    }

    @Override
    protected void rollback(Object env)
        throws IOException, InterruptedException {
    }

    @Override
    protected boolean abort(Object env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
      ByteString byteString = ByteString.copyFrom(new byte[] { 'A' });
      BytesValue state = BytesValue.newBuilder().setValue(byteString).build();
      serializer.serialize(state);
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
    }
  }

  @Test
  public void test() {
    TestProcedure procedure = new TestProcedure();
    String result = ProcedureDescriber.describe(procedure);

    Date epoch = new Date(0);

    assertEquals("{ ID => '-1', PARENT_ID => '-1', STATE => 'INITIALIZING', OWNER => '', "
        + "TYPE => 'org.apache.hadoop.hbase.procedure.TestProcedureDescriber$TestProcedure', "
        + "START_TIME => '" + epoch + "', LAST_UPDATE => '" + epoch + "', PARAMETERS => [ "
        + "{ value => 'QQ==' } ] }", result);
  }
}
