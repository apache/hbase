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

import org.apache.hadoop.hbase.spark.hbase._

/**
 * The Bound represent the boudary for the scan
 *
 * @param b The byte array of the bound
 * @param inc inclusive or not.
 */
case class Bound(b: Array[Byte], inc: Boolean)
// The non-overlapping ranges we need to scan, if lower is equal to upper, it is a get request
case class Range(lower: Option[Bound], upper: Option[Bound])
object Range {
  def apply(region: HBaseRegion): Range = {
    Range(region.start.map(Bound(_, true)), if (region.end.get.size == 0) {
      None
    } else {
      region.end.map((Bound(_, false)))
    })
  }
}

object Ranges {
  // We assume that
  // 1. r.lower.inc is true, and r.upper.inc is false
  // 2. for each range in rs, its upper.inc is false
  def and(r: Range, rs: Seq[Range]): Seq[Range] = {
    rs.flatMap{ s =>
      val lower = s.lower.map { x =>
        // the scan has lower bound
        r.lower.map { y =>
          // the region has lower bound
          if (ord.compare(x.b, y.b) < 0) {
            // scan lower bound is smaller than region server lower bound
            Some(y)
          } else {
            // scan low bound is greater or equal to region server lower bound
            Some(x)
          }
        }.getOrElse(Some(x))
      }.getOrElse(r.lower)

      val upper =  s.upper.map { x =>
        // the scan has upper bound
        r.upper.map { y =>
          // the region has upper bound
          if (ord.compare(x.b, y.b) >= 0) {
            // scan upper bound is larger than server upper bound
            // but region server scan stop is exclusive. It is OK here.
            Some(y)
          } else {
            // scan upper bound is less or equal to region server upper bound
            Some(x)
          }
        }.getOrElse(Some(x))
      }.getOrElse(r.upper)

      val c = lower.map { case x =>
        upper.map { case y =>
          ord.compare(x.b, y.b)
        }.getOrElse(-1)
      }.getOrElse(-1)
      if (c < 0) {
        Some(Range(lower, upper))
      } else {
        None
      }
    }.seq
  }
}

object Points {
  def and(r: Range, ps: Seq[Array[Byte]]): Seq[Array[Byte]] = {
    ps.flatMap { p =>
      if (ord.compare(r.lower.get.b, p) <= 0) {
        // if region lower bound is less or equal to the point
        if (r.upper.isDefined) {
          // if region upper bound is defined
          if (ord.compare(r.upper.get.b, p) > 0) {
            // if the upper bound is greater than the point (because upper bound is exclusive)
            Some(p)
          } else {
            None
          }
        } else {
          // if the region upper bound is not defined (infinity)
          Some(p)
        }
      } else {
        None
      }
    }
  }
}

