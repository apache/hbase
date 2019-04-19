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
package org.apache.hadoop.hbase.procedure2;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ServerCrashState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

@Category({MasterTests.class, SmallTests.class})
public class TestProcedureToString {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestProcedureToString.class);

  /**
   * A do-nothing environment for BasicProcedure.
   */
  static class BasicProcedureEnv {};

  /**
   * A do-nothing basic procedure just for testing toString.
   */
  static class BasicProcedure extends Procedure<BasicProcedureEnv> {
    @Override
    protected Procedure<BasicProcedureEnv>[] execute(BasicProcedureEnv env)
        throws ProcedureYieldException, InterruptedException {
      return new Procedure [] {this};
    }

    @Override
    protected void rollback(BasicProcedureEnv env) throws IOException, InterruptedException {
    }

    @Override
    protected boolean abort(BasicProcedureEnv env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer)
        throws IOException {
    }
  }

  /**
   * A do-nothing basic procedure that overrides the toStringState method. It just doubles the
   * current state string.
   */
  static class DoublingStateStringBasicProcedure extends BasicProcedure {
    @Override
    protected void toStringState(StringBuilder builder) {
      // Call twice to get the state string twice as our state value.
      super.toStringState(builder);
      super.toStringState(builder);
    }
  }

  /**
   * Test that I can override the toString for its state value.
   */
  @Test
  public void testBasicToString() {
    BasicProcedure p = new BasicProcedure();
    ProcedureState state = ProcedureState.RUNNABLE;
    p.setState(state);
    // Just assert that the toString basically works and has state in it.
    assertTrue(p.toString().contains(state.toString()));
    p = new DoublingStateStringBasicProcedure();
    p.setState(state);
    // Assert our override works and that we get double the state...
    String testStr = state.toString() + state.toString();
    assertTrue(p.toString().contains(testStr));
  }

  /**
   * Do-nothing SimpleMachineProcedure for checking its toString.
   */
  static class SimpleStateMachineProcedure
          extends StateMachineProcedure<BasicProcedureEnv, ServerCrashState> {
    @Override
    protected org.apache.hadoop.hbase.procedure2.StateMachineProcedure.Flow executeFromState(
            BasicProcedureEnv env, ServerCrashState state)
            throws ProcedureYieldException, InterruptedException {
      return null;
    }

    @Override
    protected void rollbackState(BasicProcedureEnv env, ServerCrashState state) throws IOException,
        InterruptedException {
    }

    @Override
    protected ServerCrashState getState(int stateId) {
      return ServerCrashState.valueOf(stateId);
    }

    @Override
    protected int getStateId(ServerCrashState state) {
      return state.getNumber();
    }

    @Override
    protected ServerCrashState getInitialState() {
      return null;
    }

    @Override
    protected boolean abort(BasicProcedureEnv env) {
      return false;
    }
  }

  @Test
  public void testStateMachineProcedure() {
    SimpleStateMachineProcedure p = new SimpleStateMachineProcedure();
    ProcedureState state = ProcedureState.RUNNABLE;
    p.setState(state);
    p.setNextState(ServerCrashState.SERVER_CRASH_ASSIGN);
    // Just assert that the toString basically works and has state in it.
    assertTrue(p.toString().contains(state.toString()));
    assertTrue(p.toString().contains(ServerCrashState.SERVER_CRASH_ASSIGN.toString()));
  }
}
