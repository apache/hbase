/**
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

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import com.google.protobuf.Message;

/**
 * Defines how value for specific column is interpreted and provides utility
 * methods like compare, add, multiply etc for them. Takes column family, column
 * qualifier and return the cell value. Its concrete implementation should
 * handle null case gracefully. 
 * Refer to {@link org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter} 
 * for an example.
 * <p>
 * Takes two generic parameters and three Message parameters. 
 * The cell value type of the interpreter is &lt;T&gt;.
 * During some computations like sum, average, the return type can be different
 * than the cell value data type, for eg, sum of int cell values might overflow
 * in case of a int result, we should use Long for its result. Therefore, this
 * class mandates to use a different (promoted) data type for result of these
 * computations &lt;S&gt;. All computations are performed on the promoted data type
 * &lt;S&gt;. There is a conversion method
 * {@link ColumnInterpreter#castToReturnType(Object)} which takes a &lt;T&gt; type and
 * returns a &lt;S&gt; type.
 * The AggregateIm&gt;lementation uses PB messages to initialize the
 * user's ColumnInterpreter implementation, and for sending the responses
 * back to AggregationClient.
 * @param T Cell value data type
 * @param S Promoted data type
 * @param P PB message that is used to transport initializer specific bytes
 * @param Q PB message that is used to transport Cell (&lt;T&gt;) instance
 * @param R PB message that is used to transport Promoted (&lt;S&gt;) instance
 */
@InterfaceAudience.Private
public abstract class ColumnInterpreter<T, S, P extends Message, 
Q extends Message, R extends Message> {

  /**
   * 
   * @param colFamily
   * @param colQualifier
   * @param c
   * @return value of type T
   * @throws IOException
   */
  public abstract T getValue(byte[] colFamily, byte[] colQualifier, Cell c)
      throws IOException;

  /**
   * @param l1
   * @param l2
   * @return sum or non null value among (if either of them is null); otherwise
   * returns a null.
   */
  public abstract S add(S l1, S l2);

  /**
   * returns the maximum value for this type T
   * @return max
   */

  public abstract T getMaxValue();

  public abstract T getMinValue();

  /**
   * @param o1
   * @param o2
   * @return multiplication
   */
  public abstract S multiply(S o1, S o2);

  /**
   * @param o
   * @return increment
   */
  public abstract S increment(S o);

  /**
   * provides casting opportunity between the data types.
   * @param o
   * @return cast
   */
  public abstract S castToReturnType(T o);

  /**
   * This takes care if either of arguments are null. returns 0 if they are
   * equal or both are null;
   * <ul>
   * <li>&gt; 0 if l1 &gt; l2 or l1 is not null and l2 is null.</li>
   * <li>&lt; 0 if l1 &lt; l2 or l1 is null and l2 is not null.</li>
   * </ul>
   */
  public abstract int compare(final T l1, final T l2);

  /**
   * used for computing average of &lt;S&gt; data values. Not providing the divide
   * method that takes two &lt;S&gt; values as it is not needed as of now.
   * @param o
   * @param l
   * @return Average
   */
  public abstract double divideForAvg(S o, Long l);

  /**
   * This method should return any additional data that is needed on the
   * server side to construct the ColumnInterpreter. The server
   * will pass this to the {@link #initialize}
   * method. If there is no ColumnInterpreter specific data (for e.g.,
   * {@link org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter})
   *  then null should be returned.
   * @return the PB message
   */
  public abstract P getRequestData();

  /**
   * This method should initialize any field(s) of the ColumnInterpreter with
   * a parsing of the passed message bytes (used on the server side).
   * @param msg
   */
  public abstract void initialize(P msg);
  
  /**
   * This method gets the PB message corresponding to the cell type
   * @param t
   * @return the PB message for the cell-type instance
   */
  public abstract Q getProtoForCellType(T t);

  /**
   * This method gets the PB message corresponding to the cell type
   * @param q
   * @return the cell-type instance from the PB message
   */
  public abstract T getCellValueFromProto(Q q);

  /**
   * This method gets the PB message corresponding to the promoted type
   * @param s
   * @return the PB message for the promoted-type instance
   */
  public abstract R getProtoForPromotedType(S s);

  /**
   * This method gets the promoted type from the proto message
   * @param r
   * @return the promoted-type instance from the PB message
   */
  public abstract S getPromotedValueFromProto(R r);

  /**
   * The response message comes as type S. This will convert/cast it to T.
   * In some sense, performs the opposite of {@link #castToReturnType(Object)}
   * @param response
   * @return cast
   */
  public abstract T castToCellType(S response);
}
