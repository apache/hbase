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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

@Category({ MasterTests.class, SmallTests.class })
public class TestProcedureUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestProcedureUtil.class);

  @Test
  public void testValidation() throws Exception {
    ProcedureUtil.validateClass(new TestProcedure(10));
  }

  @Test(expected = BadProcedureException.class)
  public void testNoDefaultConstructorValidation() throws Exception {
    ProcedureUtil.validateClass(new TestProcedureNoDefaultConstructor(1));
  }

  @Test
  public void testConvert() throws Exception {
    // check Procedure to protobuf conversion
    final TestProcedure proc1 = new TestProcedure(10, 1, new byte[] { 65 });
    final ProcedureProtos.Procedure proto1 = ProcedureUtil.convertToProtoProcedure(proc1);
    final TestProcedure proc2 = (TestProcedure)ProcedureUtil.convertToProcedure(proto1);
    final ProcedureProtos.Procedure proto2 = ProcedureUtil.convertToProtoProcedure(proc2);
    assertEquals(false, proto2.hasResult());
    assertEquals("Procedure protobuf does not match", proto1, proto2);
  }

  public static class TestProcedureNoDefaultConstructor extends TestProcedure {
    public TestProcedureNoDefaultConstructor(int x) {}
  }
}
