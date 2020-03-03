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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.MockMasterServices;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test CoreCoprocessor Annotation works giving access to facility not usually available.
 * Test MasterCoprocessor.
 */
@Category({CoprocessorTests.class, SmallTests.class})
public class TestCoreMasterCoprocessor {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCoreMasterCoprocessor.class);

  @Rule public TestName name = new TestName();
  private static final HBaseTestingUtility HTU = HBaseTestingUtility.createLocalHTU();
  private MasterServices ms;
  private MasterCoprocessorHost mch;

  @Before
  public void before() throws IOException {
    String methodName = this.name.getMethodName();
    this.ms = new MockMasterServices(HTU.getConfiguration(), null);
    this.mch = new MasterCoprocessorHost(this.ms, HTU.getConfiguration());
    this.mch.preMasterInitialization();
  }

  @After
  public void after() throws IOException {
    this.mch.preStopMaster();
  }

  /**
   * No annotation with CoreCoprocessor. This should make it so I can NOT get at instance of a
   * MasterServices instance after some gymnastics.
   */
  public static class NotCoreMasterCoprocessor implements MasterCoprocessor {
    public NotCoreMasterCoprocessor() {}
  }

  /**
   * Annotate with CoreCoprocessor. This should make it so I can get at instance of a
   * MasterServices instance after some gymnastics.
   */
  @CoreCoprocessor
  public static class CoreMasterCoprocessor implements MasterCoprocessor {
    public CoreMasterCoprocessor() {}
  }

  /**
   * Assert that when a Coprocessor is annotated with CoreCoprocessor, then it is possible to
   * access a MasterServices instance. Assert the opposite too.
   * Do it to MasterCoprocessors.
   * @throws IOException
   */
  @Test
  public void testCoreRegionCoprocessor() throws IOException {
    MasterCoprocessorEnvironment env =
        this.mch.load(null, NotCoreMasterCoprocessor.class.getName(), 0, HTU.getConfiguration());
    assertFalse(env instanceof HasMasterServices);
    env = this.mch.load(null, CoreMasterCoprocessor.class.getName(), 1, HTU.getConfiguration());
    assertTrue(env instanceof HasMasterServices);
    assertEquals(this.ms, ((HasMasterServices)env).getMasterServices());
  }
}
