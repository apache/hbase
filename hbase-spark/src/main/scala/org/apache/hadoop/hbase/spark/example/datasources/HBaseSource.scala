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
case class HBaseRecord(
  col0: String,
  col1: Boolean,
  col2: Double,
  col3: Float,
  col4: Int,
  col5: Long,
  col6: Short,
  col7: String,
  col8: Byte)

@InterfaceAudience.Private
object HBaseRecord {
  def apply(i: Int): HBaseRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(s,
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
object HBaseSource {
  val cat = s"""{
                |"table":{"namespace":"default", "name":"HBaseSourceExampleTable"},
                |"rowkey":"key",
                |"columns":{
                |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
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

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseSourceExample")
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

    val data = (0 to 255).map { i =>
      HBaseRecord(i)
    }

    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.hadoop.hbase.spark")
      .save()

    val df = withCatalog(cat)
    df.show()
    df.filter($"col0" <= "row005")
      .select($"col0", $"col1").show
    df.filter($"col0" === "row005" || $"col0" <= "row005")
      .select($"col0", $"col1").show
    df.filter($"col0" > "row250")
      .select($"col0", $"col1").show
    df.registerTempTable("table1")
    val c = sqlContext.sql("select count(col1) from table1 where col0 < 'row050'")
    c.show()
  }
}
