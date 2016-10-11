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

import java.util.ArrayList

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.hbase.spark.hbase._
import org.apache.hadoop.hbase.spark.datasources.HBaseResources._
import org.apache.hadoop.hbase.util.ShutdownHookManager
import org.apache.spark.sql.datasources.hbase.Field
import org.apache.spark.{SparkEnv, TaskContext, Logging, Partition}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class HBaseTableScanRDD(relation: HBaseRelation,
                       val hbaseContext: HBaseContext,
                       @transient val filter: Option[SparkSQLPushDownFilter] = None,
                        val columns: Seq[Field] = Seq.empty
     )extends RDD[Result](relation.sqlContext.sparkContext, Nil) with Logging  {
  private def sparkConf = SparkEnv.get.conf
  @transient var ranges = Seq.empty[Range]
  @transient var points = Seq.empty[Array[Byte]]
  def addPoint(p: Array[Byte]) {
    points :+= p
  }

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
      val ps = Points.and(Range(x), points)
      if (rs.size > 0 || ps.size > 0) {
        if(log.isDebugEnabled) {
          rs.foreach(x => logDebug(x.toString))
        }
        idx += 1
        Some(HBaseScanPartition(idx - 1, x, rs, ps, SerializedFilter.toSerializedTypedFilter(filter)))
      } else {
        None
      }
    }.toArray
    regions.release()
    ShutdownHookManager.affixShutdownHook( new Thread() {
      override def run() {
        HBaseConnectionCache.close()
      }
    }, 0)
    ps.asInstanceOf[Array[Partition]]
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[HBaseScanPartition].regions.server.map {
      identity
    }.toSeq
  }

  private def buildGets(
      tbr: TableResource,
      g: Seq[Array[Byte]],
      filter: Option[SparkSQLPushDownFilter],
      columns: Seq[Field],
      hbaseContext: HBaseContext): Iterator[Result] = {
    g.grouped(relation.bulkGetSize).flatMap{ x =>
      val gets = new ArrayList[Get]()
      x.foreach{ y =>
        val g = new Get(y)
        handleTimeSemantics(g)
        columns.foreach { d =>
          if (!d.isRowKey) {
            g.addColumn(d.cfBytes, d.colBytes)
          }
        }
        filter.foreach(g.setFilter(_))
        gets.add(g)
      }
      hbaseContext.applyCreds()
      val tmp = tbr.get(gets)
      rddResources.addResource(tmp)
      toResultIterator(tmp)
    }
  }

  private def toResultIterator(result: GetResource): Iterator[Result] = {
    val iterator = new Iterator[Result] {
      var idx = 0
      var cur: Option[Result] = None
      override def hasNext: Boolean = {
        while(idx < result.length && cur.isEmpty) {
          val r = result(idx)
          idx += 1
          if (!r.isEmpty) {
            cur = Some(r)
          }
        }
        if (cur.isEmpty) {
          rddResources.release(result)
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

  private def buildScan(range: Range,
      filter: Option[SparkSQLPushDownFilter],
      columns: Seq[Field]): Scan = {
    val scan = (range.lower, range.upper) match {
      case (Some(Bound(a, b)), Some(Bound(c, d))) => new Scan(a, c)
      case (None, Some(Bound(c, d))) => new Scan(Array[Byte](), c)
      case (Some(Bound(a, b)), None) => new Scan(a)
      case (None, None) => new Scan()
    }
    handleTimeSemantics(scan)

    columns.foreach { d =>
      if (!d.isRowKey) {
        scan.addColumn(d.cfBytes, d.colBytes)
      }
    }
    scan.setCacheBlocks(relation.blockCacheEnable)
    scan.setBatch(relation.batchNum)
    scan.setCaching(relation.cacheSize)
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
    val filter = SerializedFilter.fromSerializedFilter(partition.sf)
    val scans = partition.scanRanges
      .map(buildScan(_, filter, columns))
    val tableResource = TableResource(relation)
    context.addTaskCompletionListener(context => close())
    val points = partition.points
    val gIt: Iterator[Result] =  {
      if (points.isEmpty) {
        Iterator.empty: Iterator[Result]
      } else {
        buildGets(tableResource, points, filter, columns, hbaseContext)
      }
    }
    val rIts = scans.par
      .map { scan =>
      hbaseContext.applyCreds()
      val scanner = tableResource.getScanner(scan)
      rddResources.addResource(scanner)
      scanner
    }.map(toResultIterator(_))
      .fold(Iterator.empty: Iterator[Result]){ case (x, y) =>
      x ++ y
    } ++ gIt
    ShutdownHookManager.affixShutdownHook( new Thread() {
      override def run() {
        HBaseConnectionCache.close()
      }
    }, 0)
    rIts
  }

  private def handleTimeSemantics(query: Query): Unit = {
    // Set timestamp related values if present
    (query, relation.timestamp, relation.minTimestamp, relation.maxTimestamp)  match {
      case (q: Scan, Some(ts), None, None) => q.setTimeStamp(ts)
      case (q: Get, Some(ts), None, None) => q.setTimeStamp(ts)

      case (q:Scan, None, Some(minStamp), Some(maxStamp)) => q.setTimeRange(minStamp, maxStamp)
      case (q:Get, None, Some(minStamp), Some(maxStamp)) => q.setTimeRange(minStamp, maxStamp)

      case (q, None, None, None) =>

      case _ => throw new IllegalArgumentException(s"Invalid combination of query/timestamp/time range provided. " +
        s"timeStamp is: ${relation.timestamp.get}, minTimeStamp is: ${relation.minTimestamp.get}, " +
        s"maxTimeStamp is: ${relation.maxTimestamp.get}")
    }
    if (relation.maxVersions.isDefined) {
      query match {
        case q: Scan => q.setMaxVersions(relation.maxVersions.get)
        case q: Get => q.setMaxVersions(relation.maxVersions.get)
        case _ => throw new IllegalArgumentException("Invalid query provided with maxVersions")
      }
    }
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
    val points: Seq[Array[Byte]],
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
