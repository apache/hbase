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

/**
 * Dynamic logic for SQL push down logic there is an instance for most
 * common operations and a pass through for other operations not covered here
 *
 * Logic can be nested with And or Or operators.
 *
 * A logic tree can be written out as a string and reconstructed from that string
 *
 */
trait DynamicLogicExpression {
  def execute(columnToCurrentRowValueMap: util.HashMap[String, ByteArrayComparable],
              valueFromQueryValueArray:Array[Array[Byte]]): Boolean
  def toExpressionString: String = {
    val strBuilder = new StringBuilder
    appendToExpression(strBuilder)
    strBuilder.toString()
  }
  def appendToExpression(strBuilder:StringBuilder)
}

class AndLogicExpression (val leftExpression:DynamicLogicExpression,
                           val rightExpression:DynamicLogicExpression)
  extends DynamicLogicExpression{
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    leftExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray) &&
      rightExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray)
  }

  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append("( ")
    strBuilder.append(leftExpression.toExpressionString)
    strBuilder.append(" AND ")
    strBuilder.append(rightExpression.toExpressionString)
    strBuilder.append(" )")
  }
}

class OrLogicExpression (val leftExpression:DynamicLogicExpression,
                          val rightExpression:DynamicLogicExpression)
  extends DynamicLogicExpression{
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    leftExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray) ||
      rightExpression.execute(columnToCurrentRowValueMap, valueFromQueryValueArray)
  }
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append("( ")
    strBuilder.append(leftExpression.toExpressionString)
    strBuilder.append(" OR ")
    strBuilder.append(rightExpression.toExpressionString)
    strBuilder.append(" )")
  }
}

class EqualLogicExpression (val columnName:String,
                            val valueFromQueryIndex:Int,
                            val isNot:Boolean) extends DynamicLogicExpression{
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    val currentRowValue = columnToCurrentRowValueMap.get(columnName)
    val valueFromQuery = valueFromQueryValueArray(valueFromQueryIndex)

    currentRowValue != null &&
      Bytes.equals(valueFromQuery,
        0, valueFromQuery.length, currentRowValue.bytes,
        currentRowValue.offset, currentRowValue.length) != isNot
  }
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    val command = if (isNot) "!=" else "=="
    strBuilder.append(columnName + " " + command + " " + valueFromQueryIndex)
  }
}

class IsNullLogicExpression (val columnName:String,
                             val isNot:Boolean) extends DynamicLogicExpression{
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    val currentRowValue = columnToCurrentRowValueMap.get(columnName)

    (currentRowValue == null) != isNot
  }
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    val command = if (isNot) "isNotNull" else "isNull"
    strBuilder.append(columnName + " " + command)
  }
}

class GreaterThanLogicExpression (val columnName:String,
                                  val valueFromQueryIndex:Int)
  extends DynamicLogicExpression{
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    val currentRowValue = columnToCurrentRowValueMap.get(columnName)
    val valueFromQuery = valueFromQueryValueArray(valueFromQueryIndex)

    currentRowValue != null &&
      Bytes.compareTo(currentRowValue.bytes,
        currentRowValue.offset, currentRowValue.length, valueFromQuery,
        0, valueFromQuery.length) > 0
  }
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append(columnName + " > " + valueFromQueryIndex)
  }
}

class GreaterThanOrEqualLogicExpression (val columnName:String,
                                         val valueFromQueryIndex:Int)
  extends DynamicLogicExpression{
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    val currentRowValue = columnToCurrentRowValueMap.get(columnName)
    val valueFromQuery = valueFromQueryValueArray(valueFromQueryIndex)

    currentRowValue != null &&
      Bytes.compareTo(currentRowValue.bytes,
        currentRowValue.offset, currentRowValue.length, valueFromQuery,
        0, valueFromQuery.length) >= 0
  }
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append(columnName + " >= " + valueFromQueryIndex)
  }
}

class LessThanLogicExpression (val columnName:String,
                               val valueFromQueryIndex:Int)
  extends DynamicLogicExpression{
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    val currentRowValue = columnToCurrentRowValueMap.get(columnName)
    val valueFromQuery = valueFromQueryValueArray(valueFromQueryIndex)

    currentRowValue != null &&
      Bytes.compareTo(currentRowValue.bytes,
        currentRowValue.offset, currentRowValue.length, valueFromQuery,
        0, valueFromQuery.length) < 0
  }

  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append(columnName + " < " + valueFromQueryIndex)
  }
}

class LessThanOrEqualLogicExpression (val columnName:String,
                                      val valueFromQueryIndex:Int)
  extends DynamicLogicExpression{
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    val currentRowValue = columnToCurrentRowValueMap.get(columnName)
    val valueFromQuery = valueFromQueryValueArray(valueFromQueryIndex)

    currentRowValue != null &&
      Bytes.compareTo(currentRowValue.bytes,
        currentRowValue.offset, currentRowValue.length, valueFromQuery,
        0, valueFromQuery.length) <= 0
  }

  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append(columnName + " <= " + valueFromQueryIndex)
  }
}

class PassThroughLogicExpression() extends DynamicLogicExpression {
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray: Array[Array[Byte]]): Boolean = true

  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append("Pass")
  }
}

object DynamicLogicExpressionBuilder {
  def build(expressionString:String): DynamicLogicExpression = {

    val expressionAndOffset = build(expressionString.split(' '), 0)
    expressionAndOffset._1
  }

  private def build(expressionArray:Array[String],
                    offSet:Int): (DynamicLogicExpression, Int) = {
    if (expressionArray(offSet).equals("(")) {
      val left = build(expressionArray, offSet + 1)
      val right = build(expressionArray, left._2 + 1)
      if (expressionArray(left._2).equals("AND")) {
        (new AndLogicExpression(left._1, right._1), right._2 + 1)
      } else if (expressionArray(left._2).equals("OR")) {
        (new OrLogicExpression(left._1, right._1), right._2 + 1)
      } else {
        throw new Throwable("Unknown gate:" + expressionArray(left._2))
      }
    } else {
      val command = expressionArray(offSet + 1)
      if (command.equals("<")) {
        (new LessThanLogicExpression(expressionArray(offSet),
          expressionArray(offSet + 2).toInt), offSet + 3)
      } else if (command.equals("<=")) {
        (new LessThanOrEqualLogicExpression(expressionArray(offSet),
          expressionArray(offSet + 2).toInt), offSet + 3)
      } else if (command.equals(">")) {
        (new GreaterThanLogicExpression(expressionArray(offSet),
          expressionArray(offSet + 2).toInt), offSet + 3)
      } else if (command.equals(">=")) {
        (new GreaterThanOrEqualLogicExpression(expressionArray(offSet),
          expressionArray(offSet + 2).toInt), offSet + 3)
      } else if (command.equals("==")) {
        (new EqualLogicExpression(expressionArray(offSet),
          expressionArray(offSet + 2).toInt, false), offSet + 3)
      } else if (command.equals("!=")) {
        (new EqualLogicExpression(expressionArray(offSet),
          expressionArray(offSet + 2).toInt, true), offSet + 3)
      } else if (command.equals("isNull")) {
        (new IsNullLogicExpression(expressionArray(offSet), false), offSet + 2)
      } else if (command.equals("isNotNull")) {
        (new IsNullLogicExpression(expressionArray(offSet), true), offSet + 2)
      } else if (command.equals("Pass")) {
        (new PassThroughLogicExpression, offSet + 2)
      } else {
        throw new Throwable("Unknown logic command:" + command)
      }
    }
  }
}