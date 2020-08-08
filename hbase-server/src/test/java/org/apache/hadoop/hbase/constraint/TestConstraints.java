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
package org.apache.hadoop.hbase.constraint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.constraint.TestConstraint.CheckWasRunConstraint;
import org.apache.hadoop.hbase.constraint.WorksConstraint.NameConstraint;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test reading/writing the constraints into the {@link TableDescriptorBuilder}.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestConstraints {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestConstraints.class);

  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  @Test
  public void testSimpleReadWrite() throws Exception {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(name.getTableName());
    Constraints.add(builder, WorksConstraint.class);

    List<? extends Constraint> constraints =
      Constraints.getConstraints(builder.build(), this.getClass().getClassLoader());
    assertEquals(1, constraints.size());

    assertEquals(WorksConstraint.class, constraints.get(0).getClass());

    // Check that we can add more than 1 constraint and that ordering is
    // preserved
    Constraints.add(builder, AlsoWorks.class, NameConstraint.class);
    constraints = Constraints.getConstraints(builder.build(), this.getClass().getClassLoader());
    assertEquals(3, constraints.size());

    assertEquals(WorksConstraint.class, constraints.get(0).getClass());
    assertEquals(AlsoWorks.class, constraints.get(1).getClass());
    assertEquals(NameConstraint.class, constraints.get(2).getClass());

  }

  @Test
  public void testReadWriteWithConf() throws Exception {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(name.getTableName());
    Constraints.add(builder, new Pair<>(CheckConfigurationConstraint.class,
      CheckConfigurationConstraint.getConfiguration()));

    List<? extends Constraint> c =
      Constraints.getConstraints(builder.build(), this.getClass().getClassLoader());
    assertEquals(1, c.size());

    assertEquals(CheckConfigurationConstraint.class, c.get(0).getClass());

    // check to make sure that we overwrite configurations
    Constraints.add(builder,
      new Pair<>(CheckConfigurationConstraint.class, new Configuration(false)));

    try {
      Constraints.getConstraints(builder.build(), this.getClass().getClassLoader());
      fail("No exception thrown  - configuration not overwritten");
    } catch (IllegalArgumentException e) {
      // expect to have the exception, so don't do anything
    }
  }

  /**
   * Test that Constraints are properly enabled, disabled, and removed
   */
  @Test
  public void testEnableDisableRemove() throws Exception {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(name.getTableName());
    // check general enabling/disabling of constraints
    // first add a constraint
    Constraints.add(builder, AllPassConstraint.class);
    // make sure everything is enabled
    assertTrue(Constraints.enabled(builder.build(), AllPassConstraint.class));
    assertTrue(builder.hasCoprocessor(ConstraintProcessor.class.getName()));

    // check disabling
    Constraints.disable(builder);
    assertFalse(builder.hasCoprocessor(ConstraintProcessor.class.getName()));
    // make sure the added constraints are still present
    assertTrue(Constraints.enabled(builder.build(), AllPassConstraint.class));

    // check just removing the single constraint
    Constraints.remove(builder, AllPassConstraint.class);
    assertFalse(Constraints.has(builder.build(), AllPassConstraint.class));

    // Add back the single constraint
    Constraints.add(builder, AllPassConstraint.class);

    // and now check that when we remove constraints, all are gone
    Constraints.remove(builder);
    assertFalse(builder.hasCoprocessor(ConstraintProcessor.class.getName()));
    assertFalse(Constraints.has(builder.build(), AllPassConstraint.class));

  }

  /**
   * Test that when we update a constraint the ordering is not modified.
   */
  @Test
  public void testUpdateConstraint() throws Exception {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(name.getTableName());
    Constraints.add(builder, CheckConfigurationConstraint.class, CheckWasRunConstraint.class);
    Constraints.setConfiguration(builder, CheckConfigurationConstraint.class,
      CheckConfigurationConstraint.getConfiguration());

    List<? extends Constraint> constraints =
      Constraints.getConstraints(builder.build(), this.getClass().getClassLoader());

    assertEquals(2, constraints.size());

    // check to make sure the order didn't change
    assertEquals(CheckConfigurationConstraint.class, constraints.get(0).getClass());
    assertEquals(CheckWasRunConstraint.class, constraints.get(1).getClass());
  }

  /**
   * Test that if a constraint hasn't been set that there are no problems with attempting to remove
   * it.
   */
  @Test
  public void testRemoveUnsetConstraint() throws Exception {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(name.getTableName());
    Constraints.remove(builder);
    Constraints.remove(builder, AlsoWorks.class);
  }

  @Test
  public void testConfigurationPreserved() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("_ENABLED", false);
    conf.setLong("_PRIORITY", 10);
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(name.getTableName());
    Constraints.add(builder, AlsoWorks.class, conf);
    Constraints.add(builder, WorksConstraint.class);
    assertFalse(Constraints.enabled(builder.build(), AlsoWorks.class));
    List<? extends Constraint> constraints =
      Constraints.getConstraints(builder.build(), this.getClass().getClassLoader());
    for (Constraint c : constraints) {
      Configuration storedConf = c.getConf();
      if (c instanceof AlsoWorks) {
        assertEquals(10, storedConf.getLong("_PRIORITY", -1));
      }
      // its just a worksconstraint
      else {
        assertEquals(2, storedConf.getLong("_PRIORITY", -1));
      }
    }
  }

  // ---------- Constraints just used for testing

  /**
   * Also just works
   */
  public static class AlsoWorks extends BaseConstraint {
    @Override
    public void check(Put p) {
      // NOOP
    }
  }

}
