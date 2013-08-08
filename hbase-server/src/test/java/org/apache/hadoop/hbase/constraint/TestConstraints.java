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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.constraint.TestConstraint.CheckWasRunConstraint;
import org.apache.hadoop.hbase.constraint.WorksConstraint.NameConstraint;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test reading/writing the constraints into the {@link HTableDescriptor}
 */
@Category(SmallTests.class)
public class TestConstraints {

  @SuppressWarnings("unchecked")
  @Test
  public void testSimpleReadWrite() throws Throwable {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("table"));
    Constraints.add(desc, WorksConstraint.class);

    List<? extends Constraint> constraints = Constraints.getConstraints(desc,
        this.getClass().getClassLoader());
    assertEquals(1, constraints.size());

    assertEquals(WorksConstraint.class, constraints.get(0).getClass());

    // Check that we can add more than 1 constraint and that ordering is
    // preserved
    Constraints.add(desc, AlsoWorks.class, NameConstraint.class);
    constraints = Constraints.getConstraints(desc, this.getClass()
        .getClassLoader());
    assertEquals(3, constraints.size());

    assertEquals(WorksConstraint.class, constraints.get(0).getClass());
    assertEquals(AlsoWorks.class, constraints.get(1).getClass());
    assertEquals(NameConstraint.class, constraints.get(2).getClass());

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadWriteWithConf() throws Throwable {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("table"));
    Constraints.add(
        desc,
        new Pair<Class<? extends Constraint>, Configuration>(
            CheckConfigurationConstraint.class, CheckConfigurationConstraint
                .getConfiguration()));

    List<? extends Constraint> c = Constraints.getConstraints(desc, this
        .getClass().getClassLoader());
    assertEquals(1, c.size());

    assertEquals(CheckConfigurationConstraint.class, c.get(0).getClass());

    // check to make sure that we overwrite configurations
    Constraints.add(desc, new Pair<Class<? extends Constraint>, Configuration>(
        CheckConfigurationConstraint.class, new Configuration(false)));

    try {
      Constraints.getConstraints(desc, this.getClass().getClassLoader());
      fail("No exception thrown  - configuration not overwritten");
    } catch (IllegalArgumentException e) {
      // expect to have the exception, so don't do anything
    }
  }

  /**
   * Test that Constraints are properly enabled, disabled, and removed
   * 
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testEnableDisableRemove() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("table"));
    // check general enabling/disabling of constraints
    // first add a constraint
    Constraints.add(desc, AllPassConstraint.class);
    // make sure everything is enabled
    assertTrue(Constraints.enabled(desc, AllPassConstraint.class));
    assertTrue(desc.hasCoprocessor(ConstraintProcessor.class.getName()));

    // check disabling
    Constraints.disable(desc);
    assertFalse(desc.hasCoprocessor(ConstraintProcessor.class.getName()));
    // make sure the added constraints are still present
    assertTrue(Constraints.enabled(desc, AllPassConstraint.class));

    // check just removing the single constraint
    Constraints.remove(desc, AllPassConstraint.class);
    assertFalse(Constraints.has(desc, AllPassConstraint.class));

    // Add back the single constraint
    Constraints.add(desc, AllPassConstraint.class);

    // and now check that when we remove constraints, all are gone
    Constraints.remove(desc);
    assertFalse(desc.hasCoprocessor(ConstraintProcessor.class.getName()));
    assertFalse(Constraints.has(desc, AllPassConstraint.class));

  }

  /**
   * Test that when we update a constraint the ordering is not modified.
   * 
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testUpdateConstraint() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("table"));
    Constraints.add(desc, CheckConfigurationConstraint.class,
        CheckWasRunConstraint.class);
    Constraints.setConfiguration(desc, CheckConfigurationConstraint.class,
        CheckConfigurationConstraint.getConfiguration());

    List<? extends Constraint> constraints = Constraints.getConstraints(desc,
        this.getClass().getClassLoader());

    assertEquals(2, constraints.size());

    // check to make sure the order didn't change
    assertEquals(CheckConfigurationConstraint.class, constraints.get(0)
        .getClass());
    assertEquals(CheckWasRunConstraint.class, constraints.get(1).getClass());
  }

  /**
   * Test that if a constraint hasn't been set that there are no problems with
   * attempting to remove it.
   * 
   * @throws Throwable
   *           on failure.
   */
  @Test
  public void testRemoveUnsetConstraint() throws Throwable {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("table"));
    Constraints.remove(desc);
    Constraints.remove(desc, AlsoWorks.class);
  }

  @Test
  public void testConfigurationPreserved() throws Throwable {
    Configuration conf = new Configuration();
    conf.setBoolean("_ENABLED", false);
    conf.setLong("_PRIORITY", 10);
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("table"));
    Constraints.add(desc, AlsoWorks.class, conf);
    Constraints.add(desc, WorksConstraint.class);
    assertFalse(Constraints.enabled(desc, AlsoWorks.class));
    List<? extends Constraint> constraints = Constraints.getConstraints(desc,
        this.getClass().getClassLoader());
    for (Constraint c : constraints) {
      Configuration storedConf = c.getConf();
      if (c instanceof AlsoWorks)
        assertEquals(10, storedConf.getLong("_PRIORITY", -1));
      // its just a worksconstraint
      else
        assertEquals(2, storedConf.getLong("_PRIORITY", -1));

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
