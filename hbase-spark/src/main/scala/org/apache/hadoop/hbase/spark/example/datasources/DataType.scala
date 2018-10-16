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

package org.apache.hadoop.hbase.spark.example.datasources

import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.yetus.audience.InterfaceAudience

@InterfaceAudience.Private
class UserCustomizedSampleException(message: String = null, cause: Throwable = null) extends
  RuntimeException(UserCustomizedSampleException.message(message, cause), cause)

@InterfaceAudience.Private
object UserCustomizedSampleException {
  def message(message: String, cause: Throwable) =
    if (message != null) message
    else if (cause != null) cause.toString()
    else null
}

@InterfaceAudience.Private
case class IntKeyRecord(
  col0: Integer,
  col1: Boolean,
  col2: Double,
  col3: Float,
  col4: Int,
  col5: Long,
  col6: Short,
  col7: String,
  col8: Byte)

object IntKeyRecord {
  def apply(i: Int): IntKeyRecord = {
    IntKeyRecord(if (i % 2 == 0) i else -i,
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i extra",
      i.toByte)
  }
}

@InterfaceAudience.Private
object DataType {
  val cat = s"""{
                |"table":{"namespace":"default", "name":"DataTypeExampleTable"},
                |"rowkey":"key",
                |"columns":{
                |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
                |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
                |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
                |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
                |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
                |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
                |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
                |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
                |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
                |}
                |}""".stripMargin

  def main(args: Array[String]){
    val sparkConf = new SparkConf().setAppName("DataTypeExample")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
        .format("org.apache.hadoop.hbase.spark")
        .load()
    }

    // test populate table
    val data = (0 until 32).map { i =>
      IntKeyRecord(i)
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.hadoop.hbase.spark")
      .save()

    // test less than 0
    val df = withCatalog(cat)
    val s = df.filter($"col0" < 0)
    s.show()
    if(s.count() != 16){
      throw new UserCustomizedSampleException("value invalid")
    }

    //test less or equal than -10. The number of results is 11
    val num1 = df.filter($"col0" <= -10)
    num1.show()
    val c1 = num1.count()
    println(s"test result count should be 11: $c1")

    //test less or equal than -9. The number of results is 12
    val num2 = df.filter($"col0" <= -9)
    num2.show()
    val c2 = num2.count()
    println(s"test result count should be 12: $c2")

    //test greater or equal than -9". The number of results is 21
    val num3 = df.filter($"col0" >= -9)
    num3.show()
    val c3 = num3.count()
    println(s"test result count should be 21: $c3")

    //test greater or equal than 0. The number of results is 16
    val num4 = df.filter($"col0" >= 0)
    num4.show()
    val c4 = num4.count()
    println(s"test result count should be 16: $c4")

    //test greater than 10. The number of results is 10
    val num5 = df.filter($"col0" > 10)
    num5.show()
    val c5 = num5.count()
    println(s"test result count should be 10: $c5")

    // test "and". The number of results is 11
    val num6 = df.filter($"col0" > -10 && $"col0" <= 10)
    num6.show()
    val c6 = num6.count()
    println(s"test result count should be 11: $c6")

    //test "or". The number of results is 21
    val num7 = df.filter($"col0" <= -10 || $"col0" > 10)
    num7.show()
    val c7 = num7.count()
    println(s"test result count should be 21: $c7")

    //test "all". The number of results is 32
    val num8 = df.filter($"col0" >= -100)
    num8.show()
    val c8 = num8.count()
    println(s"test result count should be 32: $c8")

    //test "full query"
    val df1 = withCatalog(cat)
    df1.show()
    val c_df = df1.count()
    println(s"df count should be 32: $c_df")
    if(c_df != 32){
      throw new UserCustomizedSampleException("value invalid")
    }
  }
}
