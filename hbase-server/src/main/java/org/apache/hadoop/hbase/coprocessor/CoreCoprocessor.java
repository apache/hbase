/*
 *
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

import org.apache.yetus.audience.InterfaceAudience;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation that denotes Coprocessors that are core to HBase.
 * A Core Coprocessor is a CP that realizes a core HBase feature. Features are sometimes
 * implemented first as a Coprocessor to prove viability. The idea is that once proven, they then
 * migrate to core. Meantime, HBase Core Coprocessors get this annotation. No other Coprocessors
 * can carry this annotation.
 */
// Core Coprocessors are generally naughty making use of HBase internals doing accesses no
// Coprocessor should be up to so we mark these special Coprocessors with this annotation and on
// Coprocessor load, we'll give these Coprocessors a 'richer' Environment with access to internals
// not allowed other Coprocessors. see the *CoprocessorHost where they do the Coprocessor loadings.
@Target(ElementType.TYPE)
@Inherited
@InterfaceAudience.Private
@Retention(RetentionPolicy.RUNTIME)
// This Annotation is not @Documented because I don't want users figuring out its mechanics.
public @interface CoreCoprocessor {}
