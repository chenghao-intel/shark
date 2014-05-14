/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution.cg.row

import org.apache.hadoop.io.Writable

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.{ TypeInfoFactory => TIF }

import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspector => OI }
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{ PrimitiveObjectInspectorFactory => POIF }
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspectorFactory => OIF }

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.ql.exec.FunctionRegistry

import shark.execution.cg.SetDeferred
import shark.execution.cg.SetRaw
import shark.execution.cg.SetWritable
import shark.execution.cg.CGNotSupportDataTypeRuntimeException

/**
 * Utilities about the DataType (which is about CGField and ObjectInspector)
 */
object TypeUtil {
  private val map = scala.collection.mutable.Map[TypeInfo, DataType]()

  val NullType = new CGNull(POIF.writableVoidObjectInspector, null, -1)
  val StringType = new CGPrimitiveString(POIF.writableStringObjectInspector, null, -1)
  val BinaryType = new CGPrimitiveBinary(POIF.writableBinaryObjectInspector, null, -1)
  val IntegerType = new CGPrimitiveInt(POIF.writableIntObjectInspector, null, -1)
  val BooleanType = new CGPrimitiveBoolean(POIF.writableBooleanObjectInspector, null, -1)
  val FloatType = new CGPrimitiveFloat(POIF.writableFloatObjectInspector, null, -1)
  val DoubleType = new CGPrimitiveDouble(POIF.writableDoubleObjectInspector, null, -1)
  val LongType = new CGPrimitiveLong(POIF.writableLongObjectInspector, null, -1)
  val ByteType = new CGPrimitiveByte(POIF.writableByteObjectInspector, null, -1)
  val ShortType = new CGPrimitiveShort(POIF.writableShortObjectInspector, null, -1)
  val TimestampType = new CGPrimitiveTimestamp(POIF.writableTimestampObjectInspector, null, -1)

  register(NullType)
  register(StringType)
  register(BinaryType)
  register(IntegerType)
  register(BooleanType)
  register(FloatType)
  register(DoubleType)
  register(LongType)
  register(ByteType)
  register(ShortType)
  register(TimestampType)

  // TODO need to support the non-primitive data type, which may require creating the new
  // object inspector(union / struct)
  def register(dt: DataType) {
    map += (dt.typeInfo -> dt)
  }

  def getSetWritableClass(): Class[_] = classOf[SetWritable]
  def getDeferredObjectClass(): Class[_] = classOf[SetDeferred]
  def getSetRawClass(): Class[_] = classOf[SetRaw]

  def getDataType(ti: TypeInfo): DataType = map.get(ti) match {
    case Some(x) => x
    case None => throw new CGNotSupportDataTypeRuntimeException(ti)
  }

  def getTypeInfo(oi: OI): TypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(oi)

  def getDataType(oi: OI): DataType = if (oi.isInstanceOf[PrimitiveObjectInspector] &&
    !oi.isInstanceOf[ConstantObjectInspector])
    getDataType(getTypeInfo(oi))
  else
    CGField.create(oi, null)

  def isWritable(oi: OI): Boolean = !oi.isInstanceOf[AbstractPrimitiveJavaObjectInspector]

  def isWritable(dt: DataType): Boolean = isWritable(dt.oi)

  def standardize(dt: DataType) = getDataType(getTypeInfo(dt.oi))

  def dtToString(dt: DataType): String = {
    // TODO, need to support the HiveDecimalObjectInspector
    val oi = dt.oi
    if (oi.isInstanceOf[PrimitiveObjectInspector] && !oi.isInstanceOf[HiveDecimalObjectInspector]) {
      dt.typedOIClassName
    } else {
      throw new CGNotSupportDataTypeRuntimeException(dt)
    }
  }
  
  def dtToOIString(dt: DataType): String = {
    dt match {
      case TypeUtil.BinaryType => "PrimitivePrimitiveObjectInspectorFactory.writableBinaryObjectInspector"
      case TypeUtil.BooleanType => "PrimitiveObjectInspectorFactory.writableBooleanObjectInspector"
      case TypeUtil.ByteType => "PrimitiveObjectInspectorFactory.writableByteObjectInspector"
      case TypeUtil.DoubleType => "PrimitiveObjectInspectorFactory.writableDoubleObjectInspector"
      case TypeUtil.FloatType => "PrimitiveObjectInspectorFactory.writableFloatObjectInspector"
      case TypeUtil.IntegerType => "PrimitiveObjectInspectorFactory.writableIntObjectInspector"
      case TypeUtil.LongType => "PrimitiveObjectInspectorFactory.writableLongObjectInspector"
      case TypeUtil.ShortType => "PrimitiveObjectInspectorFactory.writableShortObjectInspector"
      case TypeUtil.StringType => "PrimitiveObjectInspectorFactory.writableStringObjectInspector"
      case TypeUtil.TimestampType => "PrimitiveObjectInspectorFactory.writableTimestampObjectInspector"
      case _ => throw new CGNotSupportDataTypeRuntimeException("couldn't support the data type:" + dt.clazz)
    }
  }

  def assertDataType(dt: DataType) {
    if (dt.isInstanceOf[CGUnion] ||
      dt.isInstanceOf[CGStruct] ||
      dt.isInstanceOf[CGMap] ||
      dt.isInstanceOf[CGList]) {
      throw new CGNotSupportDataTypeRuntimeException(dt)
    }
  }
}

// TODO the TreeNode API was from catalyst, will merge catalyst in the near future
abstract class TreeNode[BaseType <: TreeNode[BaseType]] {
  self: BaseType with Product =>

  /** Returns a Seq of the children of this node */
  def children: Seq[BaseType]
  
    /**
   * The arguments that should be included in the arg string.  Defaults to the `productIterator`.
   */
  protected def stringArgs = productIterator

  /** Returns a string representing the arguments to this node, minus any children */
  def argString: String = productIterator.flatMap {
    case tn: TreeNode[_] if children contains tn => Nil
    case tn: TreeNode[_] if tn.toString contains "\n" => s"(${tn.simpleString})" :: Nil
    case seq: Seq[_] => seq.mkString("[", ",", "]") :: Nil
    case set: Set[_] => set.mkString("{", ",", "}") :: Nil
    case other => other :: Nil
  }.mkString(", ")

  def nodeName = getClass.getSimpleName

  /** String representation of this node without any children */
  def simpleString = s"$nodeName $argString"

  override def toString: String = treeString

  /** Returns a string representation of the nodes in this tree */
  def treeString = generateTreeString(0, new StringBuilder).toString

  /**
   * Returns a string representation of the nodes in this tree, where each operator is numbered.
   * The numbers can be used with [[trees.TreeNode.apply apply]] to easily access specific subtrees.
   */
  def numberedTreeString =
    treeString.split("\n").zipWithIndex.map { case (line, i) => f"$i%02d $line" }.mkString("\n")

  /** Appends the string represent of this node and its children to the given StringBuilder. */
  protected def generateTreeString(depth: Int, builder: StringBuilder): StringBuilder = {
    builder.append(" " * depth)
    builder.append(simpleString)
    builder.append("\n")
    children.foreach(_.generateTreeString(depth + 1, builder))
    builder
  }

  /**
   * Returns a 'scala code' representation of this `TreeNode` and its children.  Intended for use
   * when debugging where the prettier toString function is obfuscating the actual structure. In the
   * case of 'pure' `TreeNodes` that only contain primitives and other TreeNodes, the result can be
   * pasted in the REPL to build an equivalent Tree.
   */
  def asCode: String = {
    val args = productIterator.map {
      case tn: TreeNode[_] => tn.asCode
      case s: String => "\"" + s + "\""
      case other => other.toString
    }
    s"$nodeName(${args.mkString(",")})"
  }
}

/**
 * A [[TreeNode]] with no children.
 */
trait LeafNode[BaseType <: TreeNode[BaseType]] {
  def children = Nil
}

/**
 * A [[TreeNode]] with a single [[child]].
 */
trait UnaryNode[BaseType <: TreeNode[BaseType]] {
  def child: BaseType
  def children = child :: Nil
}

abstract class ExprNode[NodeType <: TreeNode[NodeType]] extends TreeNode[NodeType] {
  self: NodeType with Product =>
}
