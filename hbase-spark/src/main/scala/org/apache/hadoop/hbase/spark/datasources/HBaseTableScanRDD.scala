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

package org.apache.hadoop.hbase.spark.datasources

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.spark.{ScanRange, SchemaQualifierDefinition, HBaseRelation, SparkSQLPushDownFilter}
import org.apache.hadoop.hbase.spark.hbase._
import org.apache.hadoop.hbase.spark.datasources.HBaseResources._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.{TaskContext, Logging, Partition}
import org.apache.spark.rdd.RDD

import scala.collection.mutable


class HBaseTableScanRDD(relation: HBaseRelation,
     @transient val filter: Option[SparkSQLPushDownFilter] = None,
     val columns: Seq[SchemaQualifierDefinition] = Seq.empty
     )extends RDD[Result](relation.sqlContext.sparkContext, Nil) with Logging  {
  var ranges = Seq.empty[Range]
  def addRange(r: ScanRange) = {
    val lower = if (r.lowerBound != null && r.lowerBound.length > 0) {
      Some(Bound(r.lowerBound, r.isLowerBoundEqualTo))
    } else {
      None
    }
    val upper = if (r.upperBound != null && r.upperBound.length > 0) {
      if (!r.isUpperBoundEqualTo) {
        Some(Bound(r.upperBound, false))
      } else {

        // HBase stopRow is exclusive: therefore it DOESN'T act like isUpperBoundEqualTo
        // by default.  So we need to add a new max byte to the stopRow key
        val newArray = new Array[Byte](r.upperBound.length + 1)
        System.arraycopy(r.upperBound, 0, newArray, 0, r.upperBound.length)

        //New Max Bytes
        newArray(r.upperBound.length) = ByteMin
        Some(Bound(newArray, false))
      }
    } else {
      None
    }
    ranges :+= Range(lower, upper)
  }

  override def getPartitions: Array[Partition] = {
    val regions = RegionResource(relation)
    var idx = 0
    logDebug(s"There are ${regions.size} regions")
    val ps = regions.flatMap { x =>
      val rs = Ranges.and(Range(x), ranges)
      if (rs.size > 0) {
        if(log.isDebugEnabled) {
          rs.foreach(x => logDebug(x.toString))
        }
        idx += 1
        Some(HBaseScanPartition(idx - 1, x, rs, SerializedFilter.toSerializedTypedFilter(filter)))
      } else {
        None
      }
    }.toArray
    regions.release()
    ps.asInstanceOf[Array[Partition]]
  }


  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBaseScanPartition].regions.server.map {
      identity
    }.toSeq
  }

  private def buildScan(range: Range,
      filter: Option[SparkSQLPushDownFilter],
      columns: Seq[SchemaQualifierDefinition]): Scan = {
    val scan = (range.lower, range.upper) match {
      case (Some(Bound(a, b)), Some(Bound(c, d))) => new Scan(a, c)
      case (None, Some(Bound(c, d))) => new Scan(Array[Byte](), c)
      case (Some(Bound(a, b)), None) => new Scan(a)
      case (None, None) => new Scan()
    }

    columns.foreach { d =>
      if (d.columnFamilyBytes.length > 0) {
        scan.addColumn(d.columnFamilyBytes, d.qualifierBytes)
      }
    }
    scan.setBatch(relation.batchingNum)
    scan.setCaching(relation.cachingNum)
    filter.foreach(scan.setFilter(_))
    scan
  }
  private def toResultIterator(scanner: ScanResource): Iterator[Result] = {
    val iterator = new Iterator[Result] {
      var cur: Option[Result] = None
      override def hasNext: Boolean = {
        if (cur.isEmpty) {
          val r = scanner.next()
          if (r == null) {
            rddResources.release(scanner)
          } else {
            cur = Some(r)
          }
        }
        cur.isDefined
      }
      override def next(): Result = {
        hasNext
        val ret = cur.get
        cur = None
        ret
      }
    }
    iterator
  }
  lazy val rddResources = RDDResources(new mutable.HashSet[Resource]())

  private def close() {
    rddResources.release()
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Result] = {
    val partition = split.asInstanceOf[HBaseScanPartition]

    val scans = partition.scanRanges
      .map(buildScan(_, SerializedFilter.fromSerializedFilter(partition.sf), columns))
    val tableResource = TableResource(relation)
    context.addTaskCompletionListener(context => close())
    val sIts = scans.par
      .map(tableResource.getScanner(_))
      .map(toResultIterator(_))
      .fold(Iterator.empty: Iterator[Result]){ case (x, y) =>
      x ++ y
    }
    sIts
  }
}

case class SerializedFilter(b: Option[Array[Byte]])

object SerializedFilter {
  def toSerializedTypedFilter(f: Option[SparkSQLPushDownFilter]): SerializedFilter = {
    SerializedFilter(f.map(_.toByteArray))
  }

  def fromSerializedFilter(sf: SerializedFilter): Option[SparkSQLPushDownFilter] = {
    sf.b.map(SparkSQLPushDownFilter.parseFrom(_))
  }
}

private[hbase] case class HBaseRegion(
    override val index: Int,
    val start: Option[HBaseType] = None,
    val end: Option[HBaseType] = None,
    val server: Option[String] = None) extends Partition


private[hbase] case class HBaseScanPartition(
    override val index: Int,
    val regions: HBaseRegion,
    val scanRanges: Seq[Range],
    val sf: SerializedFilter) extends Partition

case class RDDResources(set: mutable.HashSet[Resource]) {
  def addResource(s: Resource) {
    set += s
  }
  def release() {
    set.foreach(release(_))
  }
  def release(rs: Resource) {
    try {
      rs.release()
    } finally {
      set.remove(rs)
    }
  }
}
