/*
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

/**
 * Restrict the domain of a data attribute, often times to fulfill business rules/requirements.
 *
 <p>
 <h2> Table of Contents</h2>
 <ul>
 <li><a href="#overview">Overview</a></li>
 <li><a href="#concurrency">Concurrency and Atomicity</a></li>
 <li><a href="#caveats">Caveats</a></li>
 <li><a href="#usage">Example Usage</a></li>
 </ul>
 </p>

 <h2><a name="overview">Overview</a></h2>
 Constraints are used to enforce business rules in a database.
 By checking all {@link org.apache.hadoop.hbase.client.Put Puts} on a given table, you can enforce very specific data policies.
 For instance, you can ensure that a certain column family-column qualifier pair always has a value between 1 and 10.
 Otherwise, the {@link org.apache.hadoop.hbase.client.Put} is rejected and the data integrity is maintained.
 <p>
 Constraints are designed to be configurable, so a constraint can be used across different tables, but implement different
 behavior depending on the specific configuration given to that constraint.
 <p>
 By adding a constraint to a table (see <a href="#usage">Example Usage</a>), constraints will automatically enabled.
 You also then have the option of to disable (just 'turn off') or remove (delete all associated information) all constraints on a table.
 If you remove all constraints
 (see {@link org.apache.hadoop.hbase.constraint.Constraints#remove(org.apache.hadoop.hbase.HTableDescriptor)},
 you must re-add any {@link org.apache.hadoop.hbase.constraint.Constraint} you want on that table.
 However, if they are just disabled (see {@link org.apache.hadoop.hbase.constraint.Constraints#disable(org.apache.hadoop.hbase.HTableDescriptor)},
 all you need to do is enable constraints again, and everything will be turned back on as it was configured.
 Individual constraints can also be individually enabled, disabled or removed without affecting other constraints.
 <p>
 By default, constraints are disabled on a table.
 This means you will not see <i>any</i> slow down on a table if constraints are not enabled.
 <p>

 <h2><a name="concurrency">Concurrency and Atomicity</a></h2>
 Currently, no attempts at enforcing correctness in a multi-threaded scenario when modifying a constraint, via
 {@link org.apache.hadoop.hbase.constraint.Constraints}, to the the {@link org.apache.hadoop.hbase.HTableDescriptor}.
 This is particularly important when adding a constraint(s) to the {@link org.apache.hadoop.hbase.HTableDescriptor}
 as it first retrieves the next priority from a custom value set in the descriptor,
 adds each constraint (with increasing priority) to the descriptor, and then the next available priority is re-stored
 back in the {@link org.apache.hadoop.hbase.HTableDescriptor}.
 <p>
 Locking is recommended around each of Constraints add methods:
 {@link org.apache.hadoop.hbase.constraint.Constraints#add(org.apache.hadoop.hbase.HTableDescriptor, Class...)},
 {@link org.apache.hadoop.hbase.constraint.Constraints#add(org.apache.hadoop.hbase.HTableDescriptor, org.apache.hadoop.hbase.util.Pair...)},
 and {@link org.apache.hadoop.hbase.constraint.Constraints#add(org.apache.hadoop.hbase.HTableDescriptor, Class, org.apache.hadoop.conf.Configuration)}.
 Any changes on <i>a single HTableDescriptor</i> should be serialized, either within a single thread or via external mechanisms.
 <p>
 Note that having a higher priority means that a constraint will run later; e.g. a constraint with priority 1 will run before a
 constraint with priority 2.
 <p>
 Since Constraints currently are designed to just implement simple checks (e.g. is the value in the right range), there will
 be no atomicity conflicts.
 Even if one of the puts finishes the constraint first, the single row will not be corrupted and the 'fastest' write will win;
 the underlying region takes care of breaking the tie and ensuring that writes get serialized to the table.
 So yes, this doesn't ensure that we are going to get specific ordering or even a fully consistent view of the underlying data.
 <p>
 Each constraint should only use local/instance variables, unless doing more advanced usage. Static variables could cause difficulties
 when checking concurrent writes to the same region, leading to either highly locked situations (decreasing through-put) or higher probability of errors.
 However, as long as each constraint just uses local variables, each thread interacting with the constraint will execute correctly and efficiently.

 <h2><a name="caveats">Caveats</a></h2>
 In traditional (SQL) databases, Constraints are often used to enforce <a href="http://en.wikipedia.org/wiki/Relational_database#Constraints">referential integrity</a>.
 However, in HBase, this will likely cause significant overhead and dramatically decrease the number of
 {@link org.apache.hadoop.hbase.client.Put Puts}/second possible on a table. This is because to check the referential integrity
 when making a {@link org.apache.hadoop.hbase.client.Put}, one must block on a scan for the 'remote' table, checking for the valid reference.
 For millions of {@link org.apache.hadoop.hbase.client.Put Puts} a second, this will breakdown very quickly.
 There are several options around the blocking behavior including, but not limited to:
 <ul>
 <li>Create a 'pre-join' table where the keys are already denormalized</li>
 <li>Designing for 'incorrect' references</li>
 <li>Using an external enforcement mechanism</li>
 </ul>
 There are also several general considerations that must be taken into account, when using Constraints:
 <ol>
 <li>All changes made via {@link org.apache.hadoop.hbase.constraint.Constraints} will make modifications to the
 {@link org.apache.hadoop.hbase.HTableDescriptor} for a given table. As such, the usual renabling of tables should be used for
 propagating changes to the table. When at all possible, Constraints should be added to the table before the table is created.</li>
 <li>Constraints are run in the order that they are added to a table. This has implications for what order constraints should
 be added to a table.</li>
 <li>Whenever new Constraint jars are added to a region server, those region servers need to go through a rolling restart to
 make sure that they pick up the new jars and can enable the new constraints.</li>
 <li>There are certain keys that are reserved for the Configuration namespace:
 <ul>
 <li>_ENABLED - used server-side to determine if a constraint should be run</li>
 <li>_PRIORITY - used server-side to determine what order a constraint should be run</li>
 </ul>
 If these items are set, they will be respected in the constraint configuration, but they are taken care of by default in when
 adding constraints to an {@link org.apache.hadoop.hbase.HTableDescriptor} via the usual method.</li>
 </ol>
 <p>
 Under the hood, constraints are implemented as a Coprocessor (see {@link org.apache.hadoop.hbase.constraint.ConstraintProcessor}
 if you are interested).


 <h2><a name="usage">Example usage</a></h2>
 First, you must define a {@link org.apache.hadoop.hbase.constraint.Constraint}.
 The best way to do this is to extend {@link org.apache.hadoop.hbase.constraint.BaseConstraint}, which takes care of some of the more
 mundane details of using a {@link org.apache.hadoop.hbase.constraint.Constraint}.
 <p>
 Let's look at one possible implementation of a constraint - an IntegerConstraint(there are also several simple examples in the tests).
 The IntegerConstraint checks to make sure that the value is a String-encoded <code>int</code>.
 It is really simple to implement this kind of constraint, the only method needs to be implemented is
 {@link org.apache.hadoop.hbase.constraint.Constraint#check(org.apache.hadoop.hbase.client.Put)}:

 <div style="background-color: #cccccc; padding: 2px">
 <blockquote><pre>
 public class IntegerConstraint extends BaseConstraint {
 public void check(Put p) throws ConstraintException {

 Map&ltbyte[], List&ltKeyValue&gt&gt familyMap = p.getFamilyMap();

 for (List &ltKeyValue&gt kvs : familyMap.values()) {
 for (KeyValue kv : kvs) {

 // just make sure that we can actually pull out an int
 // this will automatically throw a NumberFormatException if we try to
 // store something that isn't an Integer.

 try {
 Integer.parseInt(new String(kv.getValue()));
 } catch (NumberFormatException e) {
 throw new ConstraintException("Value in Put (" + p
 + ") was not a String-encoded integer", e);
 } } }
 </pre></blockquote>
 </div>
 <p>
 Note that all exceptions that you expect to be thrown must be caught and then rethrown as a
 {@link org.apache.hadoop.hbase.constraint.ConstraintException}. This way, you can be sure that a
 {@link org.apache.hadoop.hbase.client.Put} fails for an expected reason, rather than for any reason.
 For example, an {@link java.lang.OutOfMemoryError} is probably indicative of an inherent problem in
 the {@link org.apache.hadoop.hbase.constraint.Constraint}, rather than a failed {@link org.apache.hadoop.hbase.client.Put}.
 <p>
 If an unexpected exception is thrown (for example, any kind of uncaught {@link java.lang.RuntimeException}),
 constraint-checking will be 'unloaded' from the regionserver where that error occurred.
 This means no further {@link org.apache.hadoop.hbase.constraint.Constraint Constraints} will be checked on that server
 until it is reloaded. This is done to ensure the system remains as available as possible.
 Therefore, be careful when writing your own Constraint.
 <p>
 So now that we have a Constraint, we want to add it to a table. It's as easy as:

 <div style="background-color: #cccccc; padding: 2px">
 <blockquote><pre>
 HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
 ...
 Constraints.add(desc, IntegerConstraint.class);
 </pre></blockquote></div>
 <p>
 Once we added the IntegerConstraint, constraints will be enabled on the table (once it is created) and
 we will always check to make sure that the value is an String-encoded integer.
 <p>
 However, suppose we also write our own constraint, <code>MyConstraint.java</code>.
 First, you need to make sure this class-files are in the classpath (in a jar) on the regionserver where
 that constraint will be run (this could require a rolling restart on the region server - see <a href="#caveats">Caveats</a> above)
 <p>
 Suppose that MyConstraint also uses a Configuration (see {@link org.apache.hadoop.hbase.constraint.Constraint#getConf()}).
 Then adding MyConstraint looks like this:

 <div style="background-color: #cccccc; padding: 2px">
 <blockquote><pre>
 HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
 Configuration conf = new Configuration(false);
 ...
 (add values to the conf)
 (modify the table descriptor)
 ...
 Constraints.add(desc, new Pair(MyConstraint.class, conf));
 </pre></blockquote></div>
 <p>
 At this point we added both the IntegerConstraint and MyConstraint to the table, the IntegerConstraint
 <i>will be run first</i>, followed by MyConstraint.
 <p>
 Suppose we realize that the {@link org.apache.hadoop.conf.Configuration} for MyConstraint is actually wrong
 when it was added to the table. Note, when it is added to the table, it is <i>not</i> added by reference,
 but is instead copied into the {@link org.apache.hadoop.hbase.HTableDescriptor}.
 Thus, to change the {@link org.apache.hadoop.conf.Configuration} we are using for MyConstraint, we need to do this:

 <div style="background-color: #cccccc; padding: 2px">
 <blockquote><pre>
 (add/modify the conf)
 ...
 Constraints.setConfiguration(desc, MyConstraint.class, conf);
 </pre></blockquote></div>
 <p>
 This will overwrite the previous configuration for MyConstraint, but <i>not</i> change the order of the
 constraint nor if it is enabled/disabled.
 <p>
 Note that the same constraint class can be added multiple times to a table without repercussion.
 A use case for this is the same constraint working differently based on its configuration.

 <p>
 Suppose then we want to disable <i>just</i> MyConstraint. Its as easy as:
 <div style="background-color: #cccccc">
 <blockquote><pre>
 Constraints.disable(desc, MyConstraint.class);
 </pre></blockquote></div>
 <p>
 This just turns off MyConstraint, but retains the position and the configuration associated with MyConstraint.
 Now, if we want to re-enable the constraint, its just another one-liner:
 <div style="background-color: #cccccc">
 <blockquote><pre>
 Constraints.enable(desc, MyConstraint.class);
 </pre></blockquote></div>
 <p>
 Similarly, constraints on the entire table are disabled via:
 <div style="background-color: #cccccc">
 <blockquote><pre>
 Constraints.disable(desc);
 </pre></blockquote></div>
 <p>
 Or enabled via:

 <div style="background-color: #cccccc">
 <blockquote><pre>
 Constraints.enable(desc);
 </pre></blockquote></div>
 <p>
 Lastly, suppose you want to remove MyConstraint from the table, including with position it should be run at and its configuration.
 This is similarly simple:
 <div style="background-color: #cccccc">
 <blockquote><pre>
 Constraints.remove(desc, MyConstraint.class);
 </pre></blockquote></div>
 <p>
 Also, removing <i>all</i> constraints from a table is similarly simple:
 <div style="background-color: #cccccc">
 <blockquote><pre>
 Constraints.remove(desc);
 </pre></blockquote></div>
 This will remove all constraints (and associated information) from the table and turn off the constraint processing.
 <p><b>NOTE</b><p>
 It is important to note the use above of
 <div style="background-color: #cccccc">
 <blockquote><pre>
 Configuration conf = new Configuration(false);
 </pre></blockquote></div>
 If you just use <code> new Configuration()</code>, then the Configuration will be loaded with the default
 properties. While in the simple case, this is not going to be an issue, it will cause pain down the road.
 First, these extra properties are going to cause serious bloat in your {@link org.apache.hadoop.hbase.HTableDescriptor},
 meaning you are keeping around a ton of redundant information. Second, it is going to make examining
 your table in the shell, via <code>describe 'table'</code>, a huge pain as you will have to dig through
 a ton of irrelevant config values to find the ones you set. In short, just do it the right way.
 */
package org.apache.hadoop.hbase.constraint;
