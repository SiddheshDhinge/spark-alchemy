package com.swoop.alchemy.spark.expressions.hll.factory


import com.swoop.alchemy.spark.expressions.hll.Constants
import com.swoop.alchemy.spark.expressions.hll.implementation.AgKnInstance
import com.swoop.alchemy.spark.expressions.hll.implementation.BareBonesHLLInstance
import com.swoop.alchemy.spark.expressions.hll.implementation.DataSketchesHLLInstance
import com.swoop.alchemy.spark.expressions.hll.implementation.StreamLibInstance
import org.apache.spark.sql.SparkSession


case object AgKn extends Implementation {
  override def createHll(p: Int): AgKnInstance = {
    val regWidth = {
      SparkSession.getActiveSession
        .flatMap(_.conf.getOption(Constants.Agkn.REG_WIDTH))
        .getOrElse("5").toInt
    }

    new AgKnInstance(new net.agkn.hll.HLL(p, regWidth))
  }

  override def deserialize(bytes: Array[Byte]) = new AgKnInstance(net.agkn.hll.HLL.fromBytes(bytes))
}

case object StreamLib extends Implementation {
  override def createHll(p: Int): StreamLibInstance = {
    val sparsePrecision = {
      SparkSession.getActiveSession
        .flatMap(_.conf.getOption(Constants.StrmLib.SPARSE_PRECISION))
        .getOrElse("0").toInt
    }

    new StreamLibInstance(
      new com.clearspring.analytics.stream.cardinality.HyperLogLogPlus(p, sparsePrecision)
    )
  }

  override def deserialize(bytes: Array[Byte]) = new StreamLibInstance(
    com.clearspring.analytics.stream.cardinality.HyperLogLogPlus.Builder.build(bytes)
  )
}


case object DataSketches extends Implementation {
  override def createHll(p: Int): DataSketchesHLLInstance = {
    val regWidth = {
      SparkSession.getActiveSession
        .flatMap(_.conf.getOption(Constants.DataSketchesHLL.REG_WIDTH))
        .getOrElse("6").toInt match {
          case 4 => org.apache.datasketches.hll.TgtHllType.HLL_4
          case 6 => org.apache.datasketches.hll.TgtHllType.HLL_6
          case 8 => org.apache.datasketches.hll.TgtHllType.HLL_8
        }
    }

    new DataSketchesHLLInstance(
      new org.apache.datasketches.hll.HllSketch(p, regWidth)
    )
  }

  override def deserialize(bytes: Array[Byte]) = new DataSketchesHLLInstance(
    org.apache.datasketches.hll.HllSketch.heapify(bytes)
  )
}


case object BareBonesHLL extends Implementation {
  override def createHll(p: Int): BareBonesHLLInstance = {
    val regWidth = {
      SparkSession.getActiveSession
        .flatMap(_.conf.getOption(Constants.BareBones.REG_WIDTH))
        .getOrElse("5").toInt
    }

    new BareBonesHLLInstance(
      new io.github.bareboneslib.bareboneshll.HLLPlusPlus(p, regWidth)
    )
  }

  override def deserialize(bytes: Array[Byte]) = new BareBonesHLLInstance(
    io.github.bareboneslib.bareboneshll.HLLPlusPlus.deserialize(bytes)
  )
}
