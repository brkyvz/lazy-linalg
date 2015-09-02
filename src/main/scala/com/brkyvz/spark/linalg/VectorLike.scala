package com.brkyvz.spark.linalg

import java.{util => ju}

import org.apache.spark.mllib.linalg.{SparseVector, DenseVector}

sealed trait VectorLike extends Serializable {

  /**
   * Size of the vector.
   */
  def size: Int

  def apply(i: Int): Double

  import funcs._
  def +(y: VectorLike): LazyVector = add(this, y)
  def -(y: VectorLike): LazyVector = sub(this, y)
  def *(y: VectorLike): LazyVector = emul(this, y)
  def /(y: VectorLike): LazyVector = div(this, y)

  def +(y: Scalar): LazyVector = new LazyDenseVSOp(this, y, _ + _)
  def -(y: Scalar): LazyVector = new LazyDenseVSOp(this, y, _ - _)
  def *(y: Scalar): LazyVector = LazyVectorScaleOp(y, this)
  def /(y: Scalar): LazyVector = new LazyDenseVSOp(this, y, _ / _)
}

/** Dense and Sparse Vectors can be mutated. Lazy vectors are immutable. */
sealed trait MutableVector extends VectorLike

/**
 * A dense vector represented by a value array.
 */
class DenseVectorWrapper(override val values: Array[Double])
  extends DenseVector(values) with MutableVector {

  override def foreachActive(f: (Int, Double) => Unit) = {
    var i = 0
    val localValuesSize = values.length
    val localValues = values

    while (i < localValuesSize) {
      f(i, localValues(i))
      i += 1
    }
  }

  def :=(x: LazyVector): this.type = x.compute(Option(this.values)).asInstanceOf[this.type]

  def +=(y: VectorLike): this.type = {
    y match {
      case dd: LazyMM_MV_MultOp =>
        new LazyMM_MV_MultOp(dd.left, dd.right, Option(this.values)).compute()
      case dl: LazyMM_LV_MultOp =>
        new LazyMM_LV_MultOp(dl.left, dl.right, Option(this.values)).compute()
      case ld: LazyLM_MV_MultOp =>
        new LazyLM_MV_MultOp(ld.left, ld.right, Option(this.values)).compute()
      case ll: LazyLM_LV_MultOp =>
        new LazyLM_LV_MultOp(ll.left, ll.right, Option(this.values)).compute()
      case axpy: LazyVectorScaleOp =>
        new LazyVectorAxpyOp(axpy.left, axpy.right, Option(this.values)).compute()
      case mv: MutableVector => new LazyDenseVVOp(this, mv, _ + _).compute(Option(this.values))
      case lzy: LazyVector => new LazyDenseVVOp(this, lzy, _ + _).compute(Option(this.values))
      case _ => throw new UnsupportedOperationException
    }
    this
  }
}

/**
 * A sparse vector represented by an index array and an value array.
 *
 * @param size size of the vector.
 * @param indices index array, assume to be strictly increasing.
 * @param values value array, must have the same length as the index array.
 */
class SparseVectorWrapper(
    override val size: Int,
    override val indices: Array[Int],
    override val values: Array[Double])
  extends SparseVector(size, indices, values) with MutableVector {

  override def foreachActive(f: (Int, Double) => Unit) = {
    var i = 0
    val localValuesSize = values.length
    val localIndices = indices
    val localValues = values

    while (i < localValuesSize) {
      f(localIndices(i), localValues(i))
      i += 1
    }
  }

  override def apply(i: Int): Double = {
    val index = ju.Arrays.binarySearch(indices, i)
    if (index < 0) 0.0 else values(index)
  }
}


sealed trait LazyVector extends VectorLike {
  def compute(into: Option[Array[Double]] = None): VectorLike = {
    val values = into.getOrElse(new Array[Double](size))
    require(values.length == size,
      s"Size of buffer not equal to size of vector. Buffer size: ${values.length} vs. $size")
    var i = 0
    while (i < size) {
      values(i) = this(i)
      i += 1
    }
    new DenseVectorWrapper(values)
  }
}

private[linalg] class LazyDenseVVOp(
    left: VectorLike,
    right: VectorLike,
    operation: (Double, Double) => Double) extends LazyVector {
  require(left.size == right.size,
    s"Sizes of vectors don't match. left: ${left.size} vs. right: ${right.size}")
  override def size = left.size
  override def apply(i: Int): Double = operation(left(i), right(i))
}

private[linalg] class LazyDenseVSOp(
    left: VectorLike,
    right: Scalar,
    operation: (Double, Double) => Double) extends LazyVector {
  override def size = left.size
  override def apply(i: Int): Double = operation(left(i), right.value)
}

private[linalg] class LazyDenseSVOp(
    left: Scalar,
    right: VectorLike,
    operation: (Double, Double) => Double) extends LazyVector {
  override def size = right.size
  override def apply(i: Int): Double = operation(left.value, right(i))
}

private[linalg] class LazySparseVVOp(
    left: SparseVectorWrapper,
    right: SparseVectorWrapper,
    operation: (Double, Double) => Double) extends LazyVector {
  require(left.size == right.size,
    s"Sizes of vectors don't match. left: ${left.size} vs. right: ${right.size}")
  override def size = left.size
  override def apply(i: Int): Double = operation(left(i), right(i))

  private case class IndexMatcher(index: Int, fromLeft: Boolean)

  override def compute(into: Option[Array[Double]] = None): VectorLike = {
    val leftIndices = left.indices
    val rightIndices = right.indices
    val nonZeroIndices = (leftIndices.toSet ++ rightIndices).toArray.sorted
    val numNonZeros = nonZeroIndices.length
    val values = into.getOrElse(new Array[Double](numNonZeros))
    require(values.length == numNonZeros, "Size of buffer not equal to number of non-zeros. " +
        s"Buffer size: ${values.length} vs. $numNonZeros")
    var x = 0
    var y = 0
    var z = 0
    val leftValues = left.values
    val rightValues = right.values
    while (z < numNonZeros) {
      val effLeftIndex = if (x == leftIndices.length) size else leftIndices(x)
      val effRightIndex = if (x == rightIndices.length) size else rightIndices(x)
      if (effLeftIndex == effRightIndex) {
        values(z) = operation(leftValues(x), rightValues(y))
        x += 1
        y += 1
      } else if (effLeftIndex < effRightIndex) {
        values(z) = operation(leftValues(x), 0.0)
        x += 1
      } else {
        values(z) = operation(0.0, rightValues(y))
        y += 1
      }
      z += 1
    }
    new SparseVectorWrapper(size, nonZeroIndices, values)
  }
}

private[linalg] class LazyVectorMapOp(
    parent: VectorLike,
    operation: Double => Double) extends LazyVector {
  override def size = parent.size
  override def apply(i: Int): Double = operation(parent(i))

  override def compute(into: Option[Array[Double]] = None): VectorLike = {
    parent match {
      case sp: SparseVectorWrapper =>
        val indices = sp.indices
        val nnz = indices.length
        val values = into.getOrElse(new Array[Double](nnz))
        var i = 0
        if (values.length == nnz) {
          while (i < nnz) {
            values(i) = this(indices(i))
            i += 1
          }
          new SparseVectorWrapper(size, sp.indices, values)
        } else if (values.length == size) {
          var i = 0
          while (i < size) {
            values(i) = this(i)
            i += 1
          }
          new DenseVectorWrapper(values)
        } else {
          throw new IllegalArgumentException("Size of buffer not equal to size of vector. " +
            s"Buffer size: ${values.length} vs. $nnz")
        }
      case _ =>
        val values = into.getOrElse(new Array[Double](size))
        require(values.length == size,
          s"Size of buffer not equal to size of vector. Buffer size: ${values.length} vs. $size")
        var i = 0
        while (i < size) {
          values(i) = this(i)
          i += 1
        }
        new DenseVectorWrapper(values)
    }
  }
}

private[linalg] abstract class LazyMVMultOp(
    left: MatrixLike,
    right: VectorLike,
    into: Option[Array[Double]] = None) extends LazyVector

private[linalg] case class LazyVectorAxpyOp(
    left: Scalar,
    right: VectorLike,
    into: Option[Array[Double]]) extends LazyMVMultOp(left, right, into) {
  override def size: Int = right.size

  private var buffer = into

  override def apply(i: Int): Double = result(i)

  lazy val result: VectorLike = {
    val scale = left.value
    if (scale == 1.0) {
      right match {
        case lzy: LazyVector => lzy.compute(buffer)
        case _ => right
      }
    } else {
      val inside = new DenseVectorWrapper(buffer.getOrElse(new Array[Double](size)))
      BLASUtils.axpy(scale, right, inside)
      inside
    }
  }
  override def compute(into: Option[Array[Double]] = None): VectorLike = {
    into.foreach(b => buffer = Option(b))
    result
  }
}

private[linalg] case class LazyVectorScaleOp(
    left: Scalar,
    right: VectorLike,
    into: Option[Array[Double]] = None) extends LazyMVMultOp(left, right, into) {
  override def size: Int = right.size

  private var buffer = into

  override def apply(i: Int): Double = result(i)

  lazy val result: VectorLike = {
    val scale = left.value
    if (scale == 1.0) {
      right match {
        case lzy: LazyVector => lzy.compute(buffer)
        case _ => right
      }
    } else {
      right match {
        case dn: DenseVectorWrapper =>
          buffer match {
            case Some(values) =>
              require(values.length == size,
                "Size of buffer not equal to size of vector. " +
                  s"Buffer size: ${values.length} vs. $size")
              dn.foreachActive { case (i, v) =>
                values(i) = scale * v
              }
              new DenseVectorWrapper(values)
            case None =>
              val inside = new DenseVectorWrapper(new Array[Double](size))
              BLASUtils.axpy(scale, dn, inside)
              inside
          }
        case sp: SparseVectorWrapper =>
          buffer match {
            case Some(values) =>
              if (values.length == size) {
                sp.foreachActive { case (i, v) =>
                  values(i) = scale * v
                }
                new DenseVectorWrapper(values)
              } else if (values.length == sp.values.length) {
                var i = 0
                val length = sp.values.length
                val vals = sp.values
                while (i < length) {
                  values(i) = scale * vals(i)
                  i += 1
                }
                new SparseVectorWrapper(size, sp.indices, values)
              } else {
                throw new IllegalArgumentException("The sizes of the vectors don't match for " +
                  s"scaling into. ${values.length} vs nnz: ${sp.values.length} and size: $size")
              }
            case None =>
              val inside = new DenseVectorWrapper(new Array[Double](size))
              BLASUtils.axpy(scale, sp, inside)
              inside
          }
        case lzy: LazyVector =>
          val inside = lzy.compute(buffer)
          BLASUtils.scal(scale, inside)
          inside
      }
    }
  }
  override def compute(into: Option[Array[Double]] = None): VectorLike = {
    into.foreach(b => buffer = Option(b))
    result
  }
}

private[linalg] case class LazyLM_MV_MultOp(
    left: LazyMatrix,
    right: MutableVector,
    into: Option[Array[Double]] = None) extends LazyMVMultOp(left, right, into) {
  override def size: Int = left.numRows

  override def apply(i: Int): Double = result(i)

  private var buffer = into

  lazy val result: VectorLike = {
    val inside = new DenseVectorWrapper(buffer.getOrElse(new Array[Double](size)))
    require(inside.size == size,
      s"Size of buffer not equal to size of vector. Buffer size: ${inside.size} vs. $size")
    BLASUtils.gemv(1.0, left.compute(), right, 1.0, inside)
    inside
  }
  override def compute(into: Option[Array[Double]] = None): VectorLike = {
    into.foreach(b => buffer = Option(b))
    result
  }
}

private[linalg] case class LazyLM_LV_MultOp(
    left: LazyMatrix,
    right: LazyVector,
    into: Option[Array[Double]] = None) extends LazyMVMultOp(left, right, into) {
  override def size: Int = left.numRows

  override def apply(i: Int): Double = result(i)

  private var buffer = into

  lazy val result: VectorLike = {
    var rightScale = 1.0
    val effRight: VectorLike = right match {
      case scaled: LazyVectorScaleOp =>
        rightScale = scaled.left.value
        scaled.right
      case _ => right.compute()
    }
    val inside = new DenseVectorWrapper(buffer.getOrElse(new Array[Double](size)))
    require(inside.size == size,
      s"Size of buffer not equal to size of vector. Buffer size: ${inside.size} vs. $size")
    BLASUtils.gemv(rightScale, left.compute(), effRight, 1.0, inside)
    inside
  }
  override def compute(into: Option[Array[Double]] = None): VectorLike = {
    into.foreach(b => buffer = Option(b))
    result
  }
}

private[linalg] case class LazyMM_LV_MultOp(
    left: MutableMatrix,
    right: LazyVector,
    into: Option[Array[Double]] = None) extends LazyMVMultOp(left, right, into) {
  override def size: Int = left.numRows

  override def apply(i: Int): Double = result(i)

  private var buffer = into

  lazy val result: VectorLike = {
    var rightScale = 1.0
    val effRight: VectorLike = right match {
      case scaled: LazyVectorScaleOp =>
        rightScale = scaled.left.value
        scaled.right
      case _ => right.compute()
    }
    val inside = new DenseVectorWrapper(buffer.getOrElse(new Array[Double](size)))
    require(inside.size == size,
      s"Size of buffer not equal to size of vector. Buffer size: ${inside.size} vs. $size")
    BLASUtils.gemv(rightScale, left, effRight, 1.0, inside)
    inside
  }
  override def compute(into: Option[Array[Double]] = None): VectorLike = {
    into.foreach(b => buffer = Option(b))
    result
  }
}

private[linalg] case class LazyMM_MV_MultOp(
    left: MutableMatrix,
    right: MutableVector,
    into: Option[Array[Double]] = None) extends LazyMVMultOp(left, right, into) {
  override def size: Int = left.numRows

  override def apply(i: Int): Double = result(i)

  private var buffer = into

  lazy val result: VectorLike = {
    val inside = new DenseVector(buffer.getOrElse(new Array[Double](size)))
    require(inside.size == size,
      s"Size of buffer not equal to size of vector. Buffer size: ${inside.size} vs. $size")
    BLASUtils.gemv(1.0, left, right, 1.0, inside)
    inside
  }
  override def compute(into: Option[Array[Double]] = None): VectorLike = {
    into.foreach(b => buffer = Option(b))
    result
  }
}
