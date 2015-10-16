/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.spark

import java.util

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class DynamicLogicExpressionSuite  extends FunSuite with
BeforeAndAfterEach with BeforeAndAfterAll with Logging {

  test("Basic And Test") {
    val leftLogic = new LessThanLogicExpression("Col1", 0)
    val rightLogic = new GreaterThanLogicExpression("Col1", 1)
    val andLogic = new AndLogicExpression(leftLogic, rightLogic)

    val columnToCurrentRowValueMap = new util.HashMap[String, ByteArrayComparable]()

    columnToCurrentRowValueMap.put("Col1", new ByteArrayComparable(Bytes.toBytes(10)))
    val valueFromQueryValueArray = new Array[Array[Byte]](2)
    valueFromQueryValueArray(0) = Bytes.toBytes(15)
    valueFromQueryValueArray(1) = Bytes.toBytes(5)
    assert(andLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    valueFromQueryValueArray(1) = Bytes.toBytes(5)
    assert(!andLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(15)
    valueFromQueryValueArray(1) = Bytes.toBytes(10)
    assert(!andLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    val expressionString = andLogic.toExpressionString

    assert(expressionString.equals("( Col1 < 0 AND Col1 > 1 )"))

    val builtExpression = DynamicLogicExpressionBuilder.build(expressionString)
    valueFromQueryValueArray(0) = Bytes.toBytes(15)
    valueFromQueryValueArray(1) = Bytes.toBytes(5)
    assert(builtExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    valueFromQueryValueArray(1) = Bytes.toBytes(5)
    assert(!builtExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(15)
    valueFromQueryValueArray(1) = Bytes.toBytes(10)
    assert(!builtExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

  }

  test("Basic OR Test") {
    val leftLogic = new LessThanLogicExpression("Col1", 0)
    val rightLogic = new GreaterThanLogicExpression("Col1", 1)
    val OrLogic = new OrLogicExpression(leftLogic, rightLogic)

    val columnToCurrentRowValueMap = new util.HashMap[String, ByteArrayComparable]()

    columnToCurrentRowValueMap.put("Col1", new ByteArrayComparable(Bytes.toBytes(10)))
    val valueFromQueryValueArray = new Array[Array[Byte]](2)
    valueFromQueryValueArray(0) = Bytes.toBytes(15)
    valueFromQueryValueArray(1) = Bytes.toBytes(5)
    assert(OrLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    valueFromQueryValueArray(1) = Bytes.toBytes(5)
    assert(OrLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(15)
    valueFromQueryValueArray(1) = Bytes.toBytes(10)
    assert(OrLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    valueFromQueryValueArray(1) = Bytes.toBytes(10)
    assert(!OrLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    val expressionString = OrLogic.toExpressionString

    assert(expressionString.equals("( Col1 < 0 OR Col1 > 1 )"))

    val builtExpression = DynamicLogicExpressionBuilder.build(expressionString)
    valueFromQueryValueArray(0) = Bytes.toBytes(15)
    valueFromQueryValueArray(1) = Bytes.toBytes(5)
    assert(builtExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    valueFromQueryValueArray(1) = Bytes.toBytes(5)
    assert(builtExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(15)
    valueFromQueryValueArray(1) = Bytes.toBytes(10)
    assert(builtExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    valueFromQueryValueArray(1) = Bytes.toBytes(10)
    assert(!builtExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))
  }

  test("Basic Command Test") {
    val greaterLogic = new GreaterThanLogicExpression("Col1", 0)
    val greaterAndEqualLogic = new GreaterThanOrEqualLogicExpression("Col1", 0)
    val lessLogic = new LessThanLogicExpression("Col1", 0)
    val lessAndEqualLogic = new LessThanOrEqualLogicExpression("Col1", 0)
    val equalLogic = new EqualLogicExpression("Col1", 0, false)
    val notEqualLogic = new EqualLogicExpression("Col1", 0, true)
    val passThrough = new PassThroughLogicExpression

    val columnToCurrentRowValueMap = new util.HashMap[String, ByteArrayComparable]()
    columnToCurrentRowValueMap.put("Col1", new ByteArrayComparable(Bytes.toBytes(10)))
    val valueFromQueryValueArray = new Array[Array[Byte]](1)

    //great than
    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    assert(!greaterLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(20)
    assert(!greaterLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    //great than and equal
    valueFromQueryValueArray(0) = Bytes.toBytes(5)
    assert(greaterAndEqualLogic.execute(columnToCurrentRowValueMap,
      valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    assert(greaterAndEqualLogic.execute(columnToCurrentRowValueMap,
      valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(20)
    assert(!greaterAndEqualLogic.execute(columnToCurrentRowValueMap,
      valueFromQueryValueArray))

    //less than
    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    assert(!lessLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(5)
    assert(!lessLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    //less than and equal
    valueFromQueryValueArray(0) = Bytes.toBytes(20)
    assert(lessAndEqualLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(20)
    assert(lessAndEqualLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    assert(lessAndEqualLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    //equal too
    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    assert(equalLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(5)
    assert(!equalLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    //not equal too
    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    assert(!notEqualLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(5)
    assert(notEqualLogic.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    //pass through
    valueFromQueryValueArray(0) = Bytes.toBytes(10)
    assert(passThrough.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

    valueFromQueryValueArray(0) = Bytes.toBytes(5)
    assert(passThrough.execute(columnToCurrentRowValueMap, valueFromQueryValueArray))

  }


}
