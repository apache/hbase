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
package org.apache.hadoop.hbase.classification;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


/**
 * Annotation to inform users of how much to rely on a particular package,
 * class or method not changing over time. Currently the stability can be
 * {@link Stable}, {@link Evolving} or {@link Unstable}. <br>
 *
 * <ul><li>All classes that are annotated with 
 * {@link org.apache.hadoop.hbase.classification.InterfaceAudience.Public} or
 * {@link org.apache.hadoop.hbase.classification.InterfaceAudience.LimitedPrivate} 
 * must have InterfaceStability annotation. </li>
 * <li>Classes that are 
 * {@link org.apache.hadoop.hbase.classification.InterfaceAudience.LimitedPrivate} 
 * are to be considered unstable unless a different InterfaceStability annotation
 *  states otherwise.</li>
 * <li>Incompatible changes must not be made to classes marked as stable.</li>
 * </ul>
 */
@InterfaceAudience.Public
public class InterfaceStability {
  /**
   * Can evolve while retaining compatibility for minor release boundaries.;
   * can break compatibility only at major release (ie. at m.0).
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Stable {}

  /**
   * Evolving, but can break compatibility at minor release (i.e. m.x)
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Evolving {}

  /**
   * No guarantee is provided as to reliability or stability across any
   * level of release granularity.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Unstable {}
}
