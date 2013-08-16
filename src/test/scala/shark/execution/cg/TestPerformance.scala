package shark.execution.cg

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.serde2.`lazy`.LazyLong
import org.apache.hadoop.hive.serde2.`lazy`.LazyString
import org.apache.hadoop.hive.serde2.`lazy`.LazyShort
import org.apache.hadoop.hive.serde2.`lazy`.LazyFloat
import org.apache.hadoop.hive.serde2.`lazy`.LazyDouble
import org.apache.hadoop.hive.serde2.`lazy`.LazyInteger
import org.apache.hadoop.hive.serde2.`lazy`.LazyStruct
import org.apache.hadoop.hive.serde2.`lazy`.ByteArrayRef
import org.apache.hadoop.hive.serde2.`lazy`.LazyFactory
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.junit.Test
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable


trait TestExecutor {
  def evaluate()
}

/**
 * Super Class of Test Performance Utilities
 */
class TestPerformance {
  import collection.JavaConversions._

  val data = "12,44,1,31,1234.6,1254632,1".getBytes()
  val fields: java.util.List[String] = List[String]("a", "b", "c", "d", "e", "f", "g")
  val oi = createOI()
  val cachedLazyStruct = LazyFactory.createLazyObject(oi).asInstanceOf[LazyStruct]
  val ref = createByteRef()
  val times = 20000000

  lazy val converttypes: java.util.List[TypeInfo] = List(
    TypeInfoFactory.stringTypeInfo,
    TypeInfoFactory.stringTypeInfo,
    TypeInfoFactory.stringTypeInfo,
    TypeInfoFactory.stringTypeInfo,
    TypeInfoFactory.stringTypeInfo,
    TypeInfoFactory.stringTypeInfo,
    TypeInfoFactory.stringTypeInfo)

  lazy val realTypes: java.util.List[TypeInfo] = List(
    TypeInfoFactory.longTypeInfo,
    TypeInfoFactory.longTypeInfo,
    TypeInfoFactory.longTypeInfo,
    TypeInfoFactory.shortTypeInfo,
    TypeInfoFactory.floatTypeInfo,
    TypeInfoFactory.doubleTypeInfo,
    TypeInfoFactory.intTypeInfo)

  def createOI() = LazyFactory.createLazyStructInspector(
    fields,
    types(),
    ",".getBytes(),
    new Text("\0"),
    false,
    false,
    0.asInstanceOf[Byte])

  private def createByteRef() = {
    var ref = new ByteArrayRef()
    ref.setData(data)

    ref
  }

  def measureSpeed(times: Int, exec: TestExecutor) {

    var start = System.currentTimeMillis()
    for (i <- 0 until times) exec.evaluate()
    var end = System.currentTimeMillis()

    println("Evaluation [%s] %s times, %d ms".format(exec.getClass().getName(), times, (end - start)))
  }

  def types() = converttypes
}

/**
 * Test Performance (Comparison) for Expression Evaluating & Data Parsing, which requires 
 * the data type from string to numeric
 */
class TestPerformanceWithString extends TestPerformance {
  override def types() = converttypes

  class LazyParse extends TestExecutor {
    override def evaluate() {
      cachedLazyStruct.init(ref, 0, ref.getData().length)
    }
  }

  class HiveParseWithLoop extends TestExecutor {
    var objs = new Array[Object](fields.size())

    override def evaluate() {
      cachedLazyStruct.init(ref, 0, ref.getData().length)
      for (i <- 0 until fields.size()) {
        objs(i) = cachedLazyStruct.getField(i)
      }
    }
  }

  private class HiveParse extends HiveParseWithLoop {
    override def evaluate() {
      reset()
    }

    def reset() {
      cachedLazyStruct.init(ref, 0, ref.getData().length)
      objs(0) = cachedLazyStruct.getField(0)
      objs(1) = cachedLazyStruct.getField(1)
      objs(2) = cachedLazyStruct.getField(2)
      objs(3) = cachedLazyStruct.getField(3)
      objs(4) = cachedLazyStruct.getField(4)
      objs(5) = cachedLazyStruct.getField(5)
      objs(6) = cachedLazyStruct.getField(6)
    }
  }

  private class HiveParseNativeEvaluate extends HiveParse {
    var result: Double = 0
    var aa: Long = 0
    var bb: Double = 0
    var cc: Double = 0
    var dd: Short = 0
    var ee: Float = 0
    var ff: Double = 0
    var gg: Double = 0
    var aa_c = false
    var bb_c = false
    var cc_c = false
    var dd_c = false
    var ee_c = false
    var ff_c = false
    var gg_c = false
    var result_c = false

    override def evaluate() {
      reset()
      compute()
    }

    override def reset() {
      super.reset()
      if (objs(0) != null) {
        aa_c = true
        try {
          aa = java.lang.Long.parseLong(objs(0).asInstanceOf[LazyString].getWritableObject().toString())
        } catch { case _ => aa_c = false }
      } else {
        aa_c = false
      }

      if (objs(1) != null) {
        bb_c = true
        try {
          bb = java.lang.Double.parseDouble(objs(1).asInstanceOf[LazyString].getWritableObject().toString())
        } catch { case _ => bb_c = false }
      } else {
        bb_c = false
      }

      if (objs(2) != null) {
        cc_c = true
        try {
          cc = java.lang.Double.parseDouble(objs(2).asInstanceOf[LazyString].getWritableObject().toString())
        } catch { case _ => cc_c = false }
      } else {
        cc_c = false
      }
      if (objs(3) != null) {
        dd_c = true
        try {
          dd = java.lang.Short.parseShort(objs(3).asInstanceOf[LazyString].getWritableObject().toString())
        } catch { case _ => dd_c = false }
      } else {
        dd_c = false
      }
      if (objs(4) != null) {
        ee_c = true
        try {
          ee = java.lang.Float.parseFloat(objs(4).asInstanceOf[LazyString].getWritableObject().toString())
        } catch { case _ => ee_c = false }
      } else {
        ee_c = false
      }

      if (objs(5) != null) {
        ff_c = true
        try {
          ff = java.lang.Double.parseDouble(objs(5).asInstanceOf[LazyString].getWritableObject().toString())
        } catch { case _ => ff_c = false }
      } else {
        ff_c = false
      }

      if (objs(6) != null) {
        gg_c = true
        try {
          gg = java.lang.Double.parseDouble(objs(6).asInstanceOf[LazyString].getWritableObject().toString())
        } catch { case _ => gg_c = false }
      } else {
        gg_c = false
      }
    }

    def compute() {
      if (aa_c)
        if (bb_c)
          if (cc_c)
            if (dd_c)
              if (ee_c)
                if (ff_c)
                  if (gg_c)
                    result = aa + bb + cc + dd + ee + ff + gg
                  else
                    result_c
                else
                  result_c
              else
                result_c
            else
              result_c
          else
            result_c
        else
          result_c
      else
        result_c
    }

    override def toString() = String.valueOf(result)
  }

  private class NativeEvaluate extends HiveParseNativeEvaluate {
    reset()

    override def evaluate() {
      compute()
    }
  }

  private class HiveEvaluateOnly extends TestExecutor {
    var obj: Object = _
    var desc: ExprNodeDesc = _
    var eval: ExprNodeEvaluator = _

    {
      var a = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "a", "a", false)
      var b = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "b", "a", false)
      var c = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "c", "a", false)
      var d = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "d", "a", false)
      var e = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "e", "a", false)
      var f = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "f", "a", false)
      var g = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "g", "a", false)

      desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", a,
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", b,
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", c,
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", d,
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", e,
                  TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", f, g))))))

      eval = ExprNodeEvaluatorFactory.get(desc)
      eval.initialize(oi)

      var resultO = eval.evaluate(cachedLazyStruct)
      cachedLazyStruct.init(ref, 0, ref.getData().length)
    }

    override def evaluate() {
      obj = eval.evaluate(cachedLazyStruct)
    }
  }

  private class HiveParseAndEvaluate extends HiveEvaluateOnly {
    override def evaluate() {
      cachedLazyStruct.init(ref, 0, ref.getData().length)
      obj = eval.evaluate(cachedLazyStruct)
    }
  }

  private class CGEvaluateOnly extends HiveEvaluateOnly {
    {
      eval = CGEvaluatorFactory.getEvaluator(desc)
      eval.initialize(oi)
    }

    override def evaluate() {
      obj = eval.evaluate(cachedLazyStruct)
    }
  }

  private class HiveParseAndCGEvaluate extends CGEvaluateOnly {
    override def evaluate() {
      cachedLazyStruct.init(ref, 0, ref.getData().length)
      obj = eval.evaluate(cachedLazyStruct)
    }
  }

  @Test
  def testPerformance() {
    println("CG/Hive/Direct +/- Parse(with string)")

    measureSpeed(times, new LazyParse())
    measureSpeed(times, new HiveParseWithLoop())
    measureSpeed(times, new HiveParse())
    measureSpeed(times, new NativeEvaluate())
    measureSpeed(times, new HiveParseNativeEvaluate())
    measureSpeed(times, new HiveEvaluateOnly())
    measureSpeed(times, new HiveParseAndEvaluate())
    measureSpeed(times, new CGEvaluateOnly())
    measureSpeed(times, new HiveParseAndCGEvaluate())
  }
}

/**
 * Test Performance (Comparison) for Expression Evaluating & Data Parsing, which NOT requires 
 * the data type casting
 */
class TestPerformanceWithRealNumberic extends TestPerformance {
  override def types() = realTypes

  class LazyParse extends TestExecutor {
    override def evaluate() {
      cachedLazyStruct.init(ref, 0, ref.getData().length)
    }
  }

  class HiveParseWithLoop extends TestExecutor {
    var objs = new Array[Object](fields.size())

    override def evaluate() {
      cachedLazyStruct.init(ref, 0, ref.getData().length)
      for (i <- 0 until fields.size()) {
        objs(i) = cachedLazyStruct.getField(i)
      }
    }
  }

  private class HiveParse extends HiveParseWithLoop {
    override def evaluate() {
      reset()
    }

    def reset() {
      cachedLazyStruct.init(ref, 0, ref.getData().length)
      objs(0) = cachedLazyStruct.getField(0)
      objs(1) = cachedLazyStruct.getField(1)
      objs(2) = cachedLazyStruct.getField(2)
      objs(3) = cachedLazyStruct.getField(3)
      objs(4) = cachedLazyStruct.getField(4)
      objs(5) = cachedLazyStruct.getField(5)
      objs(6) = cachedLazyStruct.getField(6)
    }
  }

  private class HiveParseNativeEvaluate extends HiveParse {
    var result: Double = 0
    var aa: Long = 0
    var bb: Long = 0
    var cc: Long = 0
    var dd: Short = 0
    var ee: Float = 0
    var ff: Double = 0
    var gg: Double = 0
    var result_c = false
    var aa_c = false
    var bb_c = false
    var cc_c = false
    var dd_c = false
    var ee_c = false
    var ff_c = false
    var gg_c = false

    override def evaluate() {
      reset()
      compute()
    }

    override def reset() {
      super.reset()

      if (objs(0) != null) {
        aa = objs(0).asInstanceOf[LazyLong].getWritableObject().get()
        aa_c = true
      } else {
        aa_c = false
      }

      if (objs(1) != null) {
        bb = objs(1).asInstanceOf[LazyLong].getWritableObject().get()
        bb_c = true
      } else {
        bb_c = false
      }

      if (objs(2) != null) {
        cc = objs(2).asInstanceOf[LazyLong].getWritableObject().get()
        cc_c = true
      } else {
        cc_c = false
      }
      if (objs(3) != null) {
        dd = objs(3).asInstanceOf[LazyShort].getWritableObject().get()
        dd_c = true
      } else {
        dd_c = false
      }
      if (objs(4) != null) {
        ee = objs(4).asInstanceOf[LazyFloat].getWritableObject().get()
        ee_c = true
      } else {
        ee_c = false
      }

      if (objs(5) != null) {
        ff = objs(5).asInstanceOf[LazyDouble].getWritableObject().get()
        ff_c = true
      } else {
        ff_c = false
      }

      if (objs(6) != null) {
        gg = objs(6).asInstanceOf[LazyInteger].getWritableObject().get()
        gg_c = true
      } else {
        gg_c = false
      }
    }

    def compute() {
      if (aa_c)
        if (bb_c)
          if (cc_c)
            if (dd_c)
              if (ee_c)
                if (ff_c)
                  if (gg_c)
                    result = aa + bb + cc + dd + ee + ff + gg
                  else
                    result_c = false
                else
                  result_c = false
              else
                result_c = false
            else
              result_c = false
          else
            result_c = false
        else
          result_c = false
      else
        result_c = false
    }

    override def toString() = String.valueOf(result)
  }

  private class NativeEvaluate extends HiveParseNativeEvaluate {
    reset()

    override def evaluate() {
      compute()
    }
  }

  private class HiveEvaluateOnly extends TestExecutor {
    var obj: Object = _
    var desc: ExprNodeDesc = _
    var eval: ExprNodeEvaluator = _

    {
      var a = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "a", "a", false)
      var b = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "b", "a", false)
      var c = new ExprNodeColumnDesc(TypeInfoFactory.longTypeInfo, "c", "a", false)
      var d = new ExprNodeColumnDesc(TypeInfoFactory.shortTypeInfo, "d", "a", false)
      var e = new ExprNodeColumnDesc(TypeInfoFactory.floatTypeInfo, "e", "a", false)
      var f = new ExprNodeColumnDesc(TypeInfoFactory.doubleTypeInfo, "f", "a", false)
      var g = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "g", "a", false)

      desc =
        TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", a,
          TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", b,
            TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", c,
              TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", d,
                TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", e,
                  TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("+", f, g))))))

      eval = ExprNodeEvaluatorFactory.get(desc)
      eval.initialize(oi)

      var resultO = eval.evaluate(cachedLazyStruct)
      cachedLazyStruct.init(ref, 0, ref.getData().length)
    }

    override def evaluate() {
      obj = eval.evaluate(cachedLazyStruct)
    }
  }

  private class HiveParseAndEvaluate extends HiveEvaluateOnly {
    override def evaluate() {
      cachedLazyStruct.init(ref, 0, ref.getData().length)
      obj = eval.evaluate(cachedLazyStruct)
    }
  }

  private class CGEvaluateOnly extends HiveEvaluateOnly {
    {
      eval = CGEvaluatorFactory.getEvaluator(desc)
      eval.initialize(oi)
    }

    override def evaluate() {
      obj = eval.evaluate(cachedLazyStruct)
    }
  }

  private class HiveParseAndCGEvaluate extends CGEvaluateOnly {
    override def evaluate() {
      cachedLazyStruct.init(ref, 0, ref.getData().length)
      obj = eval.evaluate(cachedLazyStruct)
    }
  }

  
  private class VectorizedNativeEvaluate1(val cache:Int) extends HiveParse {
    var result = new Array[Double](cache)
    var aa = new Array[Long](cache)
    var bb = new Array[Double](cache)
    var cc = new Array[Double](cache)
    var dd = new Array[Short](cache)
    var ee = new Array[Float](cache)
    var ff = new Array[Double](cache)
    var gg = new Array[Double](cache)
    
    var result_c = new Array[Boolean](cache)
    var aa_c = new Array[Boolean](cache)
    var bb_c = new Array[Boolean](cache)
    var cc_c = new Array[Boolean](cache)
    var dd_c = new Array[Boolean](cache)
    var ee_c = new Array[Boolean](cache)
    var ff_c = new Array[Boolean](cache)
    var gg_c = new Array[Boolean](cache)

    {
      for(i <- 0 until cache) {result_c(i) = true; result(i) = 0}
    }
    
    override def evaluate() {
      reset()
      compute()
    }

    override def reset() {
      for (i <- 0 until cache) {
        cachedLazyStruct.init(ref, 0, ref.getData().length)
        objs(0) = cachedLazyStruct.getField(0)
        objs(1) = cachedLazyStruct.getField(1)
        objs(2) = cachedLazyStruct.getField(2)
        objs(3) = cachedLazyStruct.getField(3)
        objs(4) = cachedLazyStruct.getField(4)
        objs(5) = cachedLazyStruct.getField(5)
        objs(6) = cachedLazyStruct.getField(6)

        if (objs(0) != null) {
          aa(i) = objs(0).asInstanceOf[LazyLong].getWritableObject().get()
          aa_c(i) = true
        } else {
          aa_c(i) = false
        }

        if (objs(1) != null) {
          bb(i) = objs(1).asInstanceOf[LazyLong].getWritableObject().get()
          bb_c(i) = true
        } else {
          bb_c(i) = false
        }

        if (objs(2) != null) {
            cc(i) = objs(2).asInstanceOf[LazyLong].getWritableObject().get()
            cc_c(i) = true
        } else {
          cc_c(i) = false
        }
        if (objs(3) != null) {
          dd(i) = objs(3).asInstanceOf[LazyShort].getWritableObject().get()
          dd_c(i) = true
        } else {
          dd_c(i) = false
        }
        if (objs(4) != null) {
          ee(i) = objs(4).asInstanceOf[LazyFloat].getWritableObject().get()
          ee_c(i) = true
        } else {
          ee_c(i) = false
        }

        if (objs(5) != null) {
          ff(i) = objs(5).asInstanceOf[LazyDouble].getWritableObject().get()
          ff_c(i) = true
        } else {
          ff_c(i) = false
        }

        if (objs(6) != null) {
            gg(i) = objs(6).asInstanceOf[LazyInteger].getWritableObject().get()
            gg_c(i) = true
        } else {
          gg_c(i) = false
        }
      }
    }

    def compute() {
      for (i <- 0 until cache) {
        if (aa_c(i))
          if (bb_c(i))
            if (cc_c(i))
              if (dd_c(i))
                if (ee_c(i))
                  if (ff_c(i))
                    if (gg_c(i))
                      result(i) = aa(i) + bb(i) + cc(i) + dd(i) + ee(i) + ff(i) + gg(i)
                    else
                      result_c(i) = false
                  else
                    result_c(i) = false
                else
                  result_c(i) = false
              else
                result_c(i) = false
            else
              result_c(i) = false
          else
            result_c(i) = false
        else
          result_c(i) = false
      }
    }

    override def toString() = String.valueOf(result)
  }
  
  private class VectorizedNativeEvaluate2(cache:Int) extends VectorizedNativeEvaluate1(cache) {
    override def compute() {
      for (i <- 0 until cache) {
        if (result_c(i) && aa_c(i)) result(i) += aa(i) else result_c(i) = false
      }
      for (i <- 0 until cache) {
        if (result_c(i) && bb_c(i)) result(i) += bb(i) else result_c(i) = false
      }
      for (i <- 0 until cache) {
        if (result_c(i) && cc_c(i)) result(i) += cc(i) else result_c(i) = false
      }
      for (i <- 0 until cache) {
        if (result_c(i) && dd_c(i)) result(i) += dd(i) else result_c(i) = false
      }
      for (i <- 0 until cache) {
        if (result_c(i) && ee_c(i)) result(i) += ee(i) else result_c(i) = false
      }
      for (i <- 0 until cache) {
        if (result_c(i) && ff_c(i)) result(i) += ff(i) else result_c(i) = false
      }
      for (i <- 0 until cache) {
        if (result_c(i) && gg_c(i)) result(i) += gg(i) else result_c(i) = false
      }
    }
  }
  
  @Test
  def testPerformance() {
    println("CG/Hive/Direct +/- Parse(with real numeric)")
    var cache = 10000
    
    measureSpeed(times, new LazyParse())
    measureSpeed(times, new HiveParseWithLoop())
    measureSpeed(times, new HiveParse())
    measureSpeed(times, new NativeEvaluate())
    measureSpeed(times, new HiveParseNativeEvaluate())
    measureSpeed(times, new HiveEvaluateOnly())
    measureSpeed(times, new HiveParseAndEvaluate())
    measureSpeed(times, new CGEvaluateOnly())
    measureSpeed(times, new HiveParseAndCGEvaluate())
    
    measureSpeed(times / cache, new VectorizedNativeEvaluate2(cache))
    measureSpeed(times / cache, new VectorizedNativeEvaluate1(cache))
  }
}

/**
 * Test Performance (Comparison) for ObjectInspector & Direct Retrieving data
 */
class TestOI extends TestPerformance {
  override def types() = realTypes

  private class OIGetExcutor extends TestExecutor {
    var soi = oi.asInstanceOf[StructObjectInspector]
    cachedLazyStruct.init(ref, 0, ref.getData().length)

    var afs = soi.getStructFieldRef("a")
    var bfs = soi.getStructFieldRef("b")
    var cfs = soi.getStructFieldRef("c")
    var dfs = soi.getStructFieldRef("d")
    var efs = soi.getStructFieldRef("e")
    var ffs = soi.getStructFieldRef("f")
    var gfs = soi.getStructFieldRef("g")

    var aoi = afs.getFieldObjectInspector().asInstanceOf[PrimitiveObjectInspector]
    var boi = bfs.getFieldObjectInspector().asInstanceOf[PrimitiveObjectInspector]
    var coi = cfs.getFieldObjectInspector().asInstanceOf[PrimitiveObjectInspector]
    var doi = dfs.getFieldObjectInspector().asInstanceOf[PrimitiveObjectInspector]
    var eoi = efs.getFieldObjectInspector().asInstanceOf[PrimitiveObjectInspector]
    var foi = ffs.getFieldObjectInspector().asInstanceOf[PrimitiveObjectInspector]
    var goi = gfs.getFieldObjectInspector().asInstanceOf[PrimitiveObjectInspector]

    var av: Long = _
    var bv: Long = _
    var cv: Long = _
    var dv: Short = _
    var ev: Float = _
    var fv: Double = _
    var gv: Int = _

    override def evaluate() {
      var aa = aoi.getPrimitiveWritableObject(soi.getStructFieldData(cachedLazyStruct, afs)).asInstanceOf[LongWritable]
      var bb = boi.getPrimitiveWritableObject(soi.getStructFieldData(cachedLazyStruct, bfs)).asInstanceOf[LongWritable]
      var cc = coi.getPrimitiveWritableObject(soi.getStructFieldData(cachedLazyStruct, cfs)).asInstanceOf[LongWritable]
      var dd = doi.getPrimitiveWritableObject(soi.getStructFieldData(cachedLazyStruct, dfs)).asInstanceOf[ShortWritable]
      var ee = eoi.getPrimitiveWritableObject(soi.getStructFieldData(cachedLazyStruct, efs)).asInstanceOf[FloatWritable]
      var ff = foi.getPrimitiveWritableObject(soi.getStructFieldData(cachedLazyStruct, ffs)).asInstanceOf[DoubleWritable]
      var gg = goi.getPrimitiveWritableObject(soi.getStructFieldData(cachedLazyStruct, gfs)).asInstanceOf[IntWritable]

      if (aa != null) av = aa.get()
      if (bb != null) bv = bb.get()
      if (cc != null) cv = cc.get()
      if (dd != null) dv = dd.get()
      if (ee != null) ev = ee.get()
      if (ff != null) fv = ff.get()
      if (gg != null) gv = gg.get()
    }

    override def toString() = "result" + av + bv + cv + dv + ev + fv + gv
  }

  private class DirectGetExcutor extends OIGetExcutor {
    override def evaluate() {
      var o = cachedLazyStruct.getField(0)

      if (o != null) {
        av = o.asInstanceOf[LazyLong].getWritableObject().get()
      }

      o = cachedLazyStruct.getField(1)
      if (o != null) {
        bv = o.asInstanceOf[LazyLong].getWritableObject().get()
      }

      o = cachedLazyStruct.getField(2)
      if (o != null) {
        cv = o.asInstanceOf[LazyLong].getWritableObject().get()
      }

      o = cachedLazyStruct.getField(3)
      if (o != null) {
        dv = o.asInstanceOf[LazyShort].getWritableObject().get()
      }

      o = cachedLazyStruct.getField(4)
      if (o != null) {
        ev = o.asInstanceOf[LazyFloat].getWritableObject().get()
      }

      o = cachedLazyStruct.getField(5)
      if (o != null) {
        fv = o.asInstanceOf[LazyDouble].getWritableObject().get()
      }

      o = cachedLazyStruct.getField(6)
      if (o != null) {
        gv = o.asInstanceOf[LazyInteger].getWritableObject().get()
      }
    }

    override def toString() = "result" + av + bv + cv + dv + ev + fv + gv
  }

  @Test
  def testOIPerformance() {
    println("ObjectInspector V.S. Direct Get")

    for (i <- 0 until 3) {
      measureSpeed(times, new OIGetExcutor())
      measureSpeed(times, new DirectGetExcutor())
    }
  }
}
