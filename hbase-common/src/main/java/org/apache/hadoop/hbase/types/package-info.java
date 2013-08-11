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

/**
 * <p>
 * This package provides the definition and implementation of HBase's
 * extensible data type API. {@link org.apache.hadoop.hbase.types.DataType}
 * is the entry point. Basic type implementations are provided based on two
 * different encoding schemes: the {@code Raw*} implementations use the
 * {@code toXXX} methods in {@link org.apache.hadoop.hbase.util.Bytes} and
 * the {@code Ordered*} implementations use the encoding scheme defined in
 * {@link org.apache.hadoop.hbase.util.OrderedBytes}. Complex types are also
 * supported in the form of {@link org.apache.hadoop.hbase.types.Struct} and
 * the abstract {@code Union} classes.
 * </p>
 * <p>
 * {@link org.apache.hadoop.hbase.types.DataType} implementations are used to
 * convert a POJO into a {@code byte[]} while maintaining application-level
 * constraints over the values produces and consumed. They also provide hints
 * to consumers about the nature of encoded values as well as the relationship
 * between different instances. See the class comments on
 * {@link org.apache.hadoop.hbase.types.DataType} for details.
 * </p>
 * <p>
 * The {@link org.apache.hadoop.hbase.types.DataType} interface is primarily
 * of use for creating rowkeys and column qualifiers. It can also be used as a
 * an encoder for primitive values. It does not support concerns of complex
 * object serialization, concepts like schema version and migration. These
 * concepts are handled more thoroughly by tools like Thrift, Avro, and
 * Protobuf.
 * </p>
 *
 * @since 0.95.2
 */
package org.apache.hadoop.hbase.types;
