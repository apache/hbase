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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.spark.datasources.{BytesEncoder, JavaBytesEncoder}
import org.apache.hadoop.hbase.spark.datasources.JavaBytesEncoder.JavaBytesEncoder
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
@InterfaceAudience.Private
trait DynamicLogicExpression {
  def execute(columnToCurrentRowValueMap: util.HashMap[String, ByteArrayComparable],
              valueFromQueryValueArray:Array[Array[Byte]]): Boolean
  def toExpressionString: String = {
    val strBuilder = new StringBuilder
    appendToExpression(strBuilder)
    strBuilder.toString()
  }
  def filterOps: JavaBytesEncoder = JavaBytesEncoder.Unknown

  def appendToExpression(strBuilder:StringBuilder)

  var encoder: BytesEncoder = _

  def setEncoder(enc: BytesEncoder): DynamicLogicExpression = {
    encoder = enc
    this
  }
}

@InterfaceAudience.Private
trait CompareTrait {
  self: DynamicLogicExpression =>
  def columnName: String
  def valueFromQueryIndex: Int
  def execute(columnToCurrentRowValueMap:
              util.HashMap[String, ByteArrayComparable],
              valueFromQueryValueArray:Array[Array[Byte]]): Boolean = {
    val currentRowValue = columnToCurrentRowValueMap.get(columnName)
    val valueFromQuery = valueFromQueryValueArray(valueFromQueryIndex)
    currentRowValue != null &&
      encoder.filter(currentRowValue.bytes, currentRowValue.offset, currentRowValue.length,
        valueFromQuery, 0, valueFromQuery.length, filterOps)
  }
}

@InterfaceAudience.Private
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

@InterfaceAudience.Private
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

@InterfaceAudience.Private
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

@InterfaceAudience.Private
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

@InterfaceAudience.Private
class GreaterThanLogicExpression (override val columnName:String,
                                  override val valueFromQueryIndex:Int)
  extends DynamicLogicExpression with CompareTrait{
  override val filterOps = JavaBytesEncoder.Greater
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append(columnName + " > " + valueFromQueryIndex)
  }
}

@InterfaceAudience.Private
class GreaterThanOrEqualLogicExpression (override val columnName:String,
                                         override val valueFromQueryIndex:Int)
  extends DynamicLogicExpression with CompareTrait{
  override val filterOps = JavaBytesEncoder.GreaterEqual
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append(columnName + " >= " + valueFromQueryIndex)
  }
}

@InterfaceAudience.Private
class LessThanLogicExpression (override val columnName:String,
                               override val valueFromQueryIndex:Int)
  extends DynamicLogicExpression with CompareTrait {
  override val filterOps = JavaBytesEncoder.Less
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append(columnName + " < " + valueFromQueryIndex)
  }
}

@InterfaceAudience.Private
class LessThanOrEqualLogicExpression (val columnName:String,
                                      val valueFromQueryIndex:Int)
  extends DynamicLogicExpression with CompareTrait{
  override val filterOps = JavaBytesEncoder.LessEqual
  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    strBuilder.append(columnName + " <= " + valueFromQueryIndex)
  }
}

@InterfaceAudience.Private
class PassThroughLogicExpression() extends DynamicLogicExpression {
  override def execute(columnToCurrentRowValueMap:
                       util.HashMap[String, ByteArrayComparable],
                       valueFromQueryValueArray: Array[Array[Byte]]): Boolean = true

  override def appendToExpression(strBuilder: StringBuilder): Unit = {
    // Fix the offset bug by add dummy to avoid crash the region server.
    // because in the DynamicLogicExpressionBuilder.build function, the command is always retrieved from offset + 1 as below
    // val command = expressionArray(offSet + 1)
    // we have to padding it so that `Pass` is on the right offset.
    strBuilder.append("dummy Pass -1")
  }
}

@InterfaceAudience.Private
object DynamicLogicExpressionBuilder {
  def build(expressionString: String, encoder: BytesEncoder): DynamicLogicExpression = {

    val expressionAndOffset = build(expressionString.split(' '), 0, encoder)
    expressionAndOffset._1
  }

  private def build(expressionArray:Array[String],
                    offSet:Int, encoder: BytesEncoder): (DynamicLogicExpression, Int) = {
    val expr = {
      if (expressionArray(offSet).equals("(")) {
        val left = build(expressionArray, offSet + 1, encoder)
        val right = build(expressionArray, left._2 + 1, encoder)
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
          (new PassThroughLogicExpression, offSet + 3)
        } else {
          throw new Throwable("Unknown logic command:" + command)
        }
      }
    }
    expr._1.setEncoder(encoder)
    expr
  }
}
