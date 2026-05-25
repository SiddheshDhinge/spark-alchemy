package com.swoop.alchemy.spark.expressions.hll.implementation

import org.apache.datasketches.hll.HllSketch
import org.apache.datasketches.hll.Union
import org.apache.datasketches.hll.TgtHllType

class DataSketchesHLLInstance(var hll: HllSketch) extends Instance {
  val union = new Union()

  override def offer(hashedValue: Long): Instance = {
    hll.update(hashedValue)
    this
  }

  override def merge(other: Instance): Instance = {
    other match {
      case instance: DataSketchesHLLInstance =>
        union.reset()
        union.update(hll)
        union.update(instance.hll)
        val tgtHllType = if(hll.getTgtHllType.ordinal() > instance.hll.getTgtHllType.ordinal())
          hll.getTgtHllType
        else
          instance.hll.getTgtHllType
        hll = union.getResult(tgtHllType)
        this
      case _ => throw new IllegalArgumentException(s"Type of HLL to merge does not match this HLL (${hll.getClass.getName})")
    }
  }

  override def serialize: Array[Byte] = hll.toCompactByteArray

  def cardinality: Long = hll.getEstimate.toLong
}