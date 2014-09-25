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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Utilities for adding/removing constraints from a table.
 * <p>
 * Constraints can be added on table load time, via the {@link HTableDescriptor}.
 * <p>
 * NOTE: this class is NOT thread safe. Concurrent setting/enabling/disabling of
 * constraints can cause constraints to be run at incorrect times or not at all.
 */
@InterfaceAudience.Private
public final class Constraints {
  private static final int DEFAULT_PRIORITY = -1;

  private Constraints() {
  }

  private static final Log LOG = LogFactory.getLog(Constraints.class);
  private static final String CONSTRAINT_HTD_KEY_PREFIX = "constraint $";
  private static final Pattern CONSTRAINT_HTD_ATTR_KEY_PATTERN = Pattern
      .compile(CONSTRAINT_HTD_KEY_PREFIX, Pattern.LITERAL);

  // configuration key for if the constraint is enabled
  private static final String ENABLED_KEY = "_ENABLED";
  // configuration key for the priority
  private static final String PRIORITY_KEY = "_PRIORITY";

  // smallest priority a constraiNt can have
  private static final long MIN_PRIORITY = 0L;
  // ensure a priority less than the smallest we could intentionally set
  private static final long UNSET_PRIORITY = MIN_PRIORITY - 1;

  private static String COUNTER_KEY = "hbase.constraint.counter";

  /**
   * Enable constraints on a table.
   * <p>
   * Currently, if you attempt to add a constraint to the table, then
   * Constraints will automatically be turned on.
   * 
   * @param desc
   *          table description to add the processor
   * @throws IOException
   *           If the {@link ConstraintProcessor} CP couldn't be added to the
   *           table.
   */
  public static void enable(HTableDescriptor desc) throws IOException {
    // if the CP has already been loaded, do nothing
    String clazz = ConstraintProcessor.class.getName();
    if (desc.hasCoprocessor(clazz)) {
      return;
    }

    // add the constrain processor CP to the table
    desc.addCoprocessor(clazz);
  }

  /**
   * Turn off processing constraints for a given table, even if constraints have
   * been turned on or added.
   * 
   * @param desc
   *          {@link HTableDescriptor} where to disable {@link Constraint
   *          Constraints}.
   */
  public static void disable(HTableDescriptor desc) {
    desc.removeCoprocessor(ConstraintProcessor.class.getName());
  }

  /**
   * Remove all {@link Constraint Constraints} that have been added to the table
   * and turn off the constraint processing.
   * <p>
   * All {@link Configuration Configurations} and their associated
   * {@link Constraint} are removed.
   * 
   * @param desc
   *          {@link HTableDescriptor} to remove {@link Constraint Constraints}
   *          from.
   */
  public static void remove(HTableDescriptor desc) {
    // disable constraints
    disable(desc);

    // remove all the constraint settings
    List<ImmutableBytesWritable> keys = new ArrayList<ImmutableBytesWritable>();
    // loop through all the key, values looking for constraints
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e : desc
        .getValues().entrySet()) {
      String key = Bytes.toString((e.getKey().get()));
      String[] className = CONSTRAINT_HTD_ATTR_KEY_PATTERN.split(key);
      if (className.length == 2) {
        keys.add(e.getKey());
      }
    }
    // now remove all the keys we found
    for (ImmutableBytesWritable key : keys) {
      desc.remove(key);
    }
  }

  /**
   * Check to see if the Constraint is currently set.
   * 
   * @param desc
   *          {@link HTableDescriptor} to check
   * @param clazz
   *          {@link Constraint} class to check for.
   * @return <tt>true</tt> if the {@link Constraint} is present, even if it is
   *         disabled. <tt>false</tt> otherwise.
   */
  public static boolean has(HTableDescriptor desc,
      Class<? extends Constraint> clazz) {
    return getKeyValueForClass(desc, clazz) != null;
  }

  /**
   * Get the kv {@link Entry} in the descriptor for the specified class
   * 
   * @param desc
   *          {@link HTableDescriptor} to read
   * @param clazz
   *          to search for
   * @return the {@link Pair} of <key, value> in the table, if that class is
   *         present. <tt>null</tt> otherwise.
   */
  private static Pair<String, String> getKeyValueForClass(
      HTableDescriptor desc, Class<? extends Constraint> clazz) {
    // get the serialized version of the constraint
    String key = serializeConstraintClass(clazz);
    String value = desc.getValue(key);

    return value == null ? null : new Pair<String, String>(key, value);
  }

  /**
   * Add configuration-less constraints to the table.
   * <p>
   * This will overwrite any configuration associated with the previous
   * constraint of the same class.
   * <p>
   * Each constraint, when added to the table, will have a specific priority,
   * dictating the order in which the {@link Constraint} will be run. A
   * {@link Constraint} earlier in the list will be run before those later in
   * the list. The same logic applies between two Constraints over time (earlier
   * added is run first on the regionserver).
   * 
   * @param desc
   *          {@link HTableDescriptor} to add {@link Constraint Constraints}
   * @param constraints
   *          {@link Constraint Constraints} to add. All constraints are
   *          considered automatically enabled on add
   * @throws IOException
   *           If constraint could not be serialized/added to table
   */
  public static void add(HTableDescriptor desc,
      Class<? extends Constraint>... constraints) throws IOException {
    // make sure constraints are enabled
    enable(desc);
    long priority = getNextPriority(desc);

    // store each constraint
    for (Class<? extends Constraint> clazz : constraints) {
      addConstraint(desc, clazz, null, priority++);
    }
    updateLatestPriority(desc, priority);
  }

  /**
   * Add constraints and their associated configurations to the table.
   * <p>
   * Adding the same constraint class twice will overwrite the first
   * constraint's configuration
   * <p>
   * Each constraint, when added to the table, will have a specific priority,
   * dictating the order in which the {@link Constraint} will be run. A
   * {@link Constraint} earlier in the list will be run before those later in
   * the list. The same logic applies between two Constraints over time (earlier
   * added is run first on the regionserver).
   * 
   * @param desc
   *          {@link HTableDescriptor} to add a {@link Constraint}
   * @param constraints
   *          {@link Pair} of a {@link Constraint} and its associated
   *          {@link Configuration}. The Constraint will be configured on load
   *          with the specified configuration.All constraints are considered
   *          automatically enabled on add
   * @throws IOException
   *           if any constraint could not be deserialized. Assumes if 1
   *           constraint is not loaded properly, something has gone terribly
   *           wrong and that all constraints need to be enforced.
   */
  public static void add(HTableDescriptor desc,
      Pair<Class<? extends Constraint>, Configuration>... constraints)
      throws IOException {
    enable(desc);
    long priority = getNextPriority(desc);
    for (Pair<Class<? extends Constraint>, Configuration> pair : constraints) {
      addConstraint(desc, pair.getFirst(), pair.getSecond(), priority++);
    }
    updateLatestPriority(desc, priority);
  }

  /**
   * Add a {@link Constraint} to the table with the given configuration
   * <p>
   * Each constraint, when added to the table, will have a specific priority,
   * dictating the order in which the {@link Constraint} will be run. A
   * {@link Constraint} added will run on the regionserver before those added to
   * the {@link HTableDescriptor} later.
   * 
   * @param desc
   *          table descriptor to the constraint to
   * @param constraint
   *          to be added
   * @param conf
   *          configuration associated with the constraint
   * @throws IOException
   *           if any constraint could not be deserialized. Assumes if 1
   *           constraint is not loaded properly, something has gone terribly
   *           wrong and that all constraints need to be enforced.
   */
  public static void add(HTableDescriptor desc,
      Class<? extends Constraint> constraint, Configuration conf)
      throws IOException {
    enable(desc);
    long priority = getNextPriority(desc);
    addConstraint(desc, constraint, conf, priority++);

    updateLatestPriority(desc, priority);
  }

  /**
   * Write the raw constraint and configuration to the descriptor.
   * <p>
   * This method takes care of creating a new configuration based on the passed
   * in configuration and then updating that with enabled and priority of the
   * constraint.
   * <p>
   * When a constraint is added, it is automatically enabled.
   */
  private static void addConstraint(HTableDescriptor desc,
      Class<? extends Constraint> clazz, Configuration conf, long priority)
      throws IOException {
    writeConstraint(desc, serializeConstraintClass(clazz),
        configure(conf, true, priority));
  }

  /**
   * Setup the configuration for a constraint as to whether it is enabled and
   * its priority
   * 
   * @param conf
   *          on which to base the new configuration
   * @param enabled
   *          <tt>true</tt> if it should be run
   * @param priority
   *          relative to other constraints
   * @return a new configuration, storable in the {@link HTableDescriptor}
   */
  private static Configuration configure(Configuration conf, boolean enabled,
      long priority) {
    // create the configuration to actually be stored
    // clone if possible, but otherwise just create an empty configuration
    Configuration toWrite = conf == null ? new Configuration()
        : new Configuration(conf);

    // update internal properties
    toWrite.setBooleanIfUnset(ENABLED_KEY, enabled);

    // set if unset long
    if (toWrite.getLong(PRIORITY_KEY, UNSET_PRIORITY) == UNSET_PRIORITY) {
      toWrite.setLong(PRIORITY_KEY, priority);
    }

    return toWrite;
  }

  /**
   * Just write the class to a String representation of the class as a key for
   * the {@link HTableDescriptor}
   * 
   * @param clazz
   *          Constraint class to convert to a {@link HTableDescriptor} key
   * @return key to store in the {@link HTableDescriptor}
   */
  private static String serializeConstraintClass(
      Class<? extends Constraint> clazz) {
    String constraintClazz = clazz.getName();
    return CONSTRAINT_HTD_KEY_PREFIX + constraintClazz;
  }

  /**
   * Write the given key and associated configuration to the
   * {@link HTableDescriptor}
   */
  private static void writeConstraint(HTableDescriptor desc, String key,
      Configuration conf) throws IOException {
    // store the key and conf in the descriptor
    desc.setValue(key, serializeConfiguration(conf));
  }

  /**
   * Write the configuration to a String
   * 
   * @param conf
   *          to write
   * @return String representation of that configuration
   * @throws IOException
   */
  private static String serializeConfiguration(Configuration conf)
      throws IOException {
    // write the configuration out to the data stream
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    conf.writeXml(dos);
    dos.flush();
    byte[] data = bos.toByteArray();
    return Bytes.toString(data);
  }

  /**
   * Read the {@link Configuration} stored in the byte stream.
   * 
   * @param bytes
   *          to read from
   * @return A valid configuration
   */
  private static Configuration readConfiguration(byte[] bytes)
      throws IOException {
    ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    Configuration conf = new Configuration(false);
    conf.addResource(is);
    return conf;
  }

  /**
   * Read in the configuration from the String encoded configuration
   * 
   * @param bytes
   *          to read from
   * @return A valid configuration
   * @throws IOException
   *           if the configuration could not be read
   */
  private static Configuration readConfiguration(String bytes)
      throws IOException {
    return readConfiguration(Bytes.toBytes(bytes));
  }

  private static long getNextPriority(HTableDescriptor desc) {
    String value = desc.getValue(COUNTER_KEY);

    long priority;
    // get the current priority
    if (value == null) {
      priority = MIN_PRIORITY;
    } else {
      priority = Long.parseLong(value) + 1;
    }

    return priority;
  }

  private static void updateLatestPriority(HTableDescriptor desc, long priority) {
    // update the max priority
    desc.setValue(COUNTER_KEY, Long.toString(priority));
  }

  /**
   * Update the configuration for the {@link Constraint}; does not change the
   * order in which the constraint is run.
   * 
   * @param desc
   *          {@link HTableDescriptor} to update
   * @param clazz
   *          {@link Constraint} to update
   * @param configuration
   *          to update the {@link Constraint} with.
   * @throws IOException
   *           if the Constraint was not stored correctly
   * @throws IllegalArgumentException
   *           if the Constraint was not present on this table.
   */
  public static void setConfiguration(HTableDescriptor desc,
      Class<? extends Constraint> clazz, Configuration configuration)
      throws IOException, IllegalArgumentException {
    // get the entry for this class
    Pair<String, String> e = getKeyValueForClass(desc, clazz);

    if (e == null) {
      throw new IllegalArgumentException("Constraint: " + clazz.getName()
          + " is not associated with this table.");
    }

    // clone over the configuration elements
    Configuration conf = new Configuration(configuration);

    // read in the previous info about the constraint
    Configuration internal = readConfiguration(e.getSecond());

    // update the fields based on the previous settings
    conf.setIfUnset(ENABLED_KEY, internal.get(ENABLED_KEY));
    conf.setIfUnset(PRIORITY_KEY, internal.get(PRIORITY_KEY));

    // update the current value
    writeConstraint(desc, e.getFirst(), conf);
  }

  /**
   * Remove the constraint (and associated information) for the table
   * descriptor.
   * 
   * @param desc
   *          {@link HTableDescriptor} to modify
   * @param clazz
   *          {@link Constraint} class to remove
   */
  public static void remove(HTableDescriptor desc,
      Class<? extends Constraint> clazz) {
    String key = serializeConstraintClass(clazz);
    desc.remove(key);
  }

  /**
   * Enable the given {@link Constraint}. Retains all the information (e.g.
   * Configuration) for the {@link Constraint}, but makes sure that it gets
   * loaded on the table.
   * 
   * @param desc
   *          {@link HTableDescriptor} to modify
   * @param clazz
   *          {@link Constraint} to enable
   * @throws IOException
   *           If the constraint cannot be properly deserialized
   */
  public static void enableConstraint(HTableDescriptor desc,
      Class<? extends Constraint> clazz) throws IOException {
    changeConstraintEnabled(desc, clazz, true);
  }

  /**
   * Disable the given {@link Constraint}. Retains all the information (e.g.
   * Configuration) for the {@link Constraint}, but it just doesn't load the
   * {@link Constraint} on the table.
   * 
   * @param desc
   *          {@link HTableDescriptor} to modify
   * @param clazz
   *          {@link Constraint} to disable.
   * @throws IOException
   *           if the constraint cannot be found
   */
  public static void disableConstraint(HTableDescriptor desc,
      Class<? extends Constraint> clazz) throws IOException {
    changeConstraintEnabled(desc, clazz, false);
  }

  /**
   * Change the whether the constraint (if it is already present) is enabled or
   * disabled.
   */
  private static void changeConstraintEnabled(HTableDescriptor desc,
      Class<? extends Constraint> clazz, boolean enabled) throws IOException {
    // get the original constraint
    Pair<String, String> entry = getKeyValueForClass(desc, clazz);
    if (entry == null) {
      throw new IllegalArgumentException("Constraint: " + clazz.getName()
          + " is not associated with this table. You can't enable it!");
    }

    // create a new configuration from that conf
    Configuration conf = readConfiguration(entry.getSecond());

    // set that it is enabled
    conf.setBoolean(ENABLED_KEY, enabled);

    // write it back out
    writeConstraint(desc, entry.getFirst(), conf);
  }

  /**
   * Check to see if the given constraint is enabled.
   * 
   * @param desc
   *          {@link HTableDescriptor} to check.
   * @param clazz
   *          {@link Constraint} to check for
   * @return <tt>true</tt> if the {@link Constraint} is present and enabled.
   *         <tt>false</tt> otherwise.
   * @throws IOException
   *           If the constraint has improperly stored in the table
   */
  public static boolean enabled(HTableDescriptor desc,
      Class<? extends Constraint> clazz) throws IOException {
    // get the kv
    Pair<String, String> entry = getKeyValueForClass(desc, clazz);
    // its not enabled so just return false. In fact, its not even present!
    if (entry == null) {
      return false;
    }

    // get the info about the constraint
    Configuration conf = readConfiguration(entry.getSecond());

    return conf.getBoolean(ENABLED_KEY, false);
  }

  /**
   * Get the constraints stored in the table descriptor
   * 
   * @param desc
   *          To read from
   * @param classloader
   *          To use when loading classes. If a special classloader is used on a
   *          region, for instance, then that should be the classloader used to
   *          load the constraints. This could also apply to unit-testing
   *          situation, where want to ensure that class is reloaded or not.
   * @return List of configured {@link Constraint Constraints}
   * @throws IOException
   *           if any part of reading/arguments fails
   */
  static List<? extends Constraint> getConstraints(HTableDescriptor desc,
      ClassLoader classloader) throws IOException {
    List<Constraint> constraints = new ArrayList<Constraint>();
    // loop through all the key, values looking for constraints
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e : desc
        .getValues().entrySet()) {
      // read out the constraint
      String key = Bytes.toString(e.getKey().get()).trim();
      String[] className = CONSTRAINT_HTD_ATTR_KEY_PATTERN.split(key);
      if (className.length == 2) {
        key = className[1];
        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading constraint:" + key);
        }

        // read in the rest of the constraint
        Configuration conf;
        try {
          conf = readConfiguration(e.getValue().get());
        } catch (IOException e1) {
          // long that we don't have a valid configuration stored, and move on.
          LOG.warn("Corrupted configuration found for key:" + key
              + ",  skipping it.");
          continue;
        }
        // if it is not enabled, skip it
        if (!conf.getBoolean(ENABLED_KEY, false)) {
          if (LOG.isDebugEnabled())
            LOG.debug("Constraint: " + key + " is DISABLED - skipping it");
          // go to the next constraint
          continue;
        }

        try {
          // add the constraint, now that we expect it to be valid.
          Class<? extends Constraint> clazz = classloader.loadClass(key)
              .asSubclass(Constraint.class);
          Constraint constraint = clazz.newInstance();
          constraint.setConf(conf);
          constraints.add(constraint);
        } catch (ClassNotFoundException e1) {
          throw new IOException(e1);
        } catch (InstantiationException e1) {
          throw new IOException(e1);
        } catch (IllegalAccessException e1) {
          throw new IOException(e1);
        }
      }
    }
    // sort them, based on the priorities
    Collections.sort(constraints, constraintComparator);
    return constraints;
  }

  private static final Comparator<Constraint> constraintComparator = new Comparator<Constraint>() {
    @Override
    public int compare(Constraint c1, Constraint c2) {
      // compare the priorities of the constraints stored in their configuration
      return Long.valueOf(c1.getConf().getLong(PRIORITY_KEY, DEFAULT_PRIORITY))
          .compareTo(c2.getConf().getLong(PRIORITY_KEY, DEFAULT_PRIORITY));
    }
  };

}
