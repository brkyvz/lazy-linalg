package com.brkyvz.spark.linalg

import org.apache.spark.mllib.linalg.{SparseMatrix, DenseMatrix}

trait MatrixLike {

  /** Number of rows. */
  def numRows: Int

  /** Number of columns. */
  def numCols: Int

  def size: Int = numRows * numCols

  def apply(i: Int): Double

  import MatrixOperators._
  def +(y: MatrixLike): LazyMatrix = add(this, y)
  def -(y: MatrixLike): LazyMatrix = sub(this, y)
  def :*(y: MatrixLike): LazyMatrix = mul(this, y)
  def *(y: Scalar): LazyMatrix = mul(this, y)
  def /(y: MatrixLike): LazyMatrix = div(this, y)
}

/** Dense and Sparse Matrices can be mutated. Lazy matrices are immutable. */
trait MutableMatrix extends MatrixLike

class DenseMatrixWrapper(
    override val numRows: Int,
    override val numCols: Int,
    override val values: Array[Double],
    override val isTransposed: Boolean)
  extends DenseMatrix(numRows, numCols, values, isTransposed) with MutableMatrix {

  def this(numRows: Int, numCols: Int, values: Array[Double]) =
    this(numRows, numCols, values, isTransposed = false)

  override def apply(i: Int): Double = values(i)

  def *(y: DenseMatrix): LazyMatrix = new LazyMM_MMultOp(this, y)
  def *(y: LazyMatrix): LazyMatrix = new LazyML_MMultOp(this, y)
  def +=(y: LazyMM_MMultOp): this.type = {
    new LazyMM_MMultOp(y.left, y.right, Option(this)).compute()
    this
  }
  def +=(y: LazyML_MMultOp): this.type = {
    new LazyML_MMultOp(y.left, y.right, Option(this)).compute()
    this
  }
  def +=(y: LazyLM_MMultOp): this.type = {
    new LazyLM_MMultOp(y.left, y.right, Option(this)).compute()
    this
  }
  def +=(y: LazyLL_MMultOp): this.type = {
    new LazyLL_MMultOp(y.left, y.right, Option(this)).compute()
    this
  }
  def +=(y: LazyMatrix): this.type = {
    y match {
      case dd: LazyMM_MMultOp => this += dd
      case dl: LazyML_MMultOp => this += dl
      case ld: LazyLM_MMultOp => this += ld
      case ll: LazyLL_MMultOp => this += ll
      case _ => throw new IllegalArgumentException
    }
    this
  }

  def :=(y: LazyMatrix): MatrixLike = {
    y.compute(Option(this.values))
  }
}

object DenseMatrixWrapper {
  def apply(mat: DenseMatrix): DenseMatrixWrapper =
    new DenseMatrixWrapper(mat.numRows, mat.numCols, mat.values, mat.isTransposed)
}

class SparseMatrixWrapper(
    override val numRows: Int,
    override val numCols: Int,
    override val colPtrs: Array[Int],
    override val rowIndices: Array[Int],
    override val values: Array[Double],
    override val isTransposed: Boolean)
  extends SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)
  with MutableMatrix {

  def this(
      numRows: Int,
      numCols: Int,
      colPtrs: Array[Int],
      rowIndices: Array[Int],
      values: Array[Double]) =
    this(numRows, numCols, colPtrs, rowIndices, values, isTransposed = false)

  override def apply(i: Int): Double = values(i)
}

object SparseMatrixWrapper {
  def apply(mat: SparseMatrix): SparseMatrixWrapper = new SparseMatrixWrapper(mat.numRows,
    mat.numCols, mat.colPtrs, mat.rowIndices, mat.values, mat.isTransposed)
}

trait LazyMatrix extends MatrixLike {
  def compute(into: Option[Array[Double]] = None): MatrixLike = {
    val values = into.getOrElse(new Array[Double](size))
    var i = 0
    while (i < size) {
      values(i) = this(i)
      i += 1
    }
    new DenseMatrixWrapper(numRows, numCols, values)
  }

  def *(y: DenseMatrix): LazyMatrix = new LazyLM_MMultOp(this, y)
  def *(y: SparseMatrix): LazyMatrix = new LazyLM_MMultOp(this, y)
  def *(y: LazyMatrix): LazyMatrix = new LazyLL_MMultOp(this, y)
}

abstract class LazyMMOp(
    left: MatrixLike,
    right: MatrixLike,
    operation: (Double, Double) => Double) extends LazyMatrix {
  override def numRows = math.max(left.numRows, right.numRows)
  override def numCols = math.max(left.numCols, right.numCols)
}

class LazyImDenseMMOp(
    left: MatrixLike,
    right: MatrixLike,
    operation: (Double, Double) => Double) extends LazyMMOp(left, right, operation) {
  override def apply(i: Int): Double = operation(left(i), right(i))
}

case class LazyImDenseScaleOp(
    left: MatrixLikeDouble,
    right: MatrixLike) extends LazyImDenseMMOp(left, right, _ * _)

abstract class LazyMOp(parent: MatrixLike,
                       operation: Double => Double) extends LazyMatrix {
  override def numRows = parent.numRows
  override def numCols = parent.numCols
}

class LazyImDenseMOp(
    parent: MatrixLike,
    operation: Double => Double) extends LazyMOp(parent, operation) {
  override def apply(i: Int): Double = operation(parent(i))
}

abstract class LazyMMultOp(
    left: MatrixLike,
    right: MatrixLike,
    into: Option[DenseMatrixWrapper] = None) extends LazyMatrix {
  override def numRows = left.numRows
  override def numCols = right.numCols
}

class LazyLL_MMultOp(
    val left: LazyMatrix,
    val right: LazyMatrix,
    into: Option[DenseMatrixWrapper] = None) extends LazyMMultOp(left, right, into) {
  override def apply(i: Int): Double = result(i)

  lazy val result: DenseMatrixWrapper = {
    var leftScale = 1.0
    val (effLeft: DenseMatrixWrapper, leftRes) = left match {
      case scaled: LazyImDenseScaleOp =>
        leftScale = scaled.left.value
        (scaled.right, None)
      case ll: LazyLL_MMultOp =>
        if (ll.size < ll.right.size) {
          (ll.compute(), None)
        } else {
          (ll.right.compute(), Option(ll.left))
        }
      case ld: LazyLM_MMultOp =>
        if (ld.size < ld.right.size) {
          (ld.compute(), None)
        } else {
          (ld.right, Option(ld.left))
        }
      case dl: LazyML_MMultOp =>
        if (dl.size < dl.right.size) {
          (dl.compute(), None)
        } else {
          (dl.right.compute(), Option(dl.left))
        }
      case dd: LazyMM_MMultOp =>
        if (dd.size < dd.right.size) {
          (dd.compute(), None)
        } else {
          (dd.right, Option(dd.left))
        }
      case _ => (left.compute(), None)
    }
    var rightScale = 1.0
    val (effRight: DenseMatrixWrapper, rightRes) = right match {
      case scaled: LazyImDenseScaleOp =>
        rightScale = scaled.left.value
        (scaled.right, None)
      case ll: LazyLL_MMultOp =>
        if (ll.size < ll.right.size) {
          (ll.compute(), None)
        } else {
          (ll.right.compute(), Option(ll.left))
        }
      case ld: LazyLM_MMultOp =>
        if (ld.size < ld.right.size) {
          (ld.compute(), None)
        } else {
          (ld.right, Option(ld.left))
        }
      case dl: LazyML_MMultOp =>
        if (dl.size < dl.right.size) {
          (dl.compute(), None)
        } else {
          (dl.right.compute(), Option(dl.left))
        }
      case dd: LazyMM_MMultOp =>
        if (dd.size < dd.right.size) {
          (dd.compute(), None)
        } else {
          (dd.right, Option(dd.left))
        }
      case _ => (right.compute(), None)
    }
    val middle =
      if (leftRes == None && rightRes == None) {
        val inside = into.getOrElse(DenseMatrix.zeros(effLeft.numRows, effRight.numCols))
        BLASUtils.gemm(leftScale * rightScale, effLeft, effRight, 1.0, inside)
        inside
      } else {
        val inside = DenseMatrix.zeros(effLeft.numRows, effRight.numCols)
        BLASUtils.gemm(leftScale * rightScale, effLeft, effRight, 1.0, inside)
        inside
      }

    val rebuildRight = rightRes.getOrElse(None) match {
      case l: LazyMatrix => new LazyML_MMultOp(middle, l)
      case d: DenseMatrixWrapper => new LazyMM_MMultOp(middle, d)
      case None => middle
    }
    leftRes.getOrElse(None) match {
      case l: LazyMatrix =>
        rebuildRight match {
          case r: LazyMatrix => new LazyLL_MMultOp(l, r, into).compute()
          case d: DenseMatrixWrapper => new LazyLM_MMultOp(l, d, into).compute()
        }
      case ld: DenseMatrixWrapper =>
        rebuildRight match {
          case r: LazyMatrix => new LazyML_MMultOp(ld, r, into).compute()
          case d: DenseMatrixWrapper => new LazyMM_MMultOp(ld, d, into).compute()
        }
      case None =>
        rebuildRight match {
          case r: LazyMM_MMultOp => new LazyMM_MMultOp(r.left, r.right, into).compute()
          case l: LazyML_MMultOp => new LazyML_MMultOp(l.left, l.right, into).compute()
          case d: DenseMatrixWrapper => d
        }
    }
  }
  override def compute(into: Option[Array[Double]] = None): DenseMatrixWrapper = result
}

class LazyLM_MMultOp(
    val left: LazyMatrix,
    val right: MutableMatrix,
    into: Option[DenseMatrixWrapper] = None) extends LazyMMultOp(left, right, into) {
  override def apply(i: Int): Double = result(i)

  lazy val result: DenseMatrixWrapper = {
    var leftScale = 1.0
    val (effLeft: DenseMatrixWrapper, leftRes) = left match {
      case scaled: LazyImDenseScaleOp =>
        leftScale = scaled.left.value
        (scaled.right, None)
      case ll: LazyLL_MMultOp =>
        if (ll.size < ll.right.size) {
          (ll.compute(), None)
        } else {
          (ll.right.compute(), Option(ll.left))
        }
      case ld: LazyLM_MMultOp =>
        if (ld.size < ld.right.size) {
          (ld.compute(), None)
        } else {
          (ld.right, Option(ld.left))
        }
      case dl: LazyML_MMultOp =>
        if (dl.size < dl.right.size) {
          (dl.compute(), None)
        } else {
          (dl.right.compute(), Option(dl.left))
        }
      case dd: LazyMM_MMultOp =>
        if (dd.size < dd.right.size) {
          (dd.compute(), None)
        } else {
          (dd.right, Option(dd.left))
        }
      case _ => (left.compute(), None)
    }

    val middle =
      if (leftRes == None) {
        val inside = into.getOrElse(DenseMatrix.zeros(effLeft.numRows, right.numCols))
        BLASUtils.gemm(leftScale, effLeft, right, 1.0, inside)
        inside
      } else {
        val inside = DenseMatrix.zeros(effLeft.numRows, right.numCols)
        BLASUtils.gemm(leftScale, effLeft, right, 1.0, inside)
        inside
      }

    leftRes.getOrElse(None) match {
      case l: LazyMatrix => new LazyLM_MMultOp(l, middle, into).compute()
      case ld: DenseMatrixWrapper => new LazyMM_MMultOp(ld, middle, into).compute()
      case None => middle
    }
  }

  override def compute(into: Option[Array[Double]] = None): DenseMatrixWrapper = result
}

class LazyML_MMultOp(
    val left: MutableMatrix,
    val right: LazyMatrix,
    into: Option[DenseMatrixWrapper] = None) extends LazyMMultOp(left, right, into) {
  override def apply(i: Int): Double = result(i)

  lazy val result: DenseMatrixWrapper = {
    var rightScale = 1.0
    val (effRight: DenseMatrixWrapper, rightRes) = right match {
      case scaled: LazyImDenseScaleOp =>
        rightScale = scaled.left.value
        (scaled.right, None)
      case ll: LazyLL_MMultOp =>
        if (ll.size < ll.right.size) {
          (ll.compute(), None)
        } else {
          (ll.right.compute(), Option(ll.left))
        }
      case ld: LazyLM_MMultOp =>
        if (ld.size < ld.right.size) {
          (ld.compute(), None)
        } else {
          (ld.right, Option(ld.left))
        }
      case dl: LazyML_MMultOp =>
        if (dl.size < dl.right.size) {
          (dl.compute(), None)
        } else {
          (dl.right.compute(), Option(dl.left))
        }
      case dd: LazyMM_MMultOp =>
        if (dd.size < dd.right.size) {
          (dd.compute(), None)
        } else {
          (dd.right, Option(dd.left))
        }
      case _ => (right.compute(), None)
    }
    val middle =
      if (rightRes == None) {
        val inside = into.getOrElse(DenseMatrix.zeros(left.numRows, effRight.numCols))
        BLASUtils.gemm(rightScale, left, effRight, 1.0, inside)
        inside
      } else {
        val inside = DenseMatrix.zeros(left.numRows, effRight.numCols)
        BLASUtils.gemm(rightScale, left, effRight, 0.0, inside)
        inside
      }

    rightRes.getOrElse(None) match {
      case l: LazyMatrix => new LazyML_MMultOp(middle, l, into).compute()
      case d: DenseMatrixWrapper => new LazyMM_MMultOp(middle, d, into).compute()
      case None => middle
    }
  }

  override def compute(into: Option[Array[Double]] = None): DenseMatrixWrapper = result
}

class LazyMM_MMultOp(
    val left: MutableMatrix,
    val right: MutableMatrix,
    into: Option[DenseMatrixWrapper] = None) extends LazyMMultOp(left, right, into) {
  override def apply(i: Int): Double = result(i)
  lazy val result: DenseMatrixWrapper = {
    val inside = into.getOrElse(DenseMatrix.zeros(left.numRows, right.numCols))
    BLASUtils.gemm(1.0, left, right, 1.0, inside)
    inside
  }

  override def compute(into: Option[Array[Double]] = None): DenseMatrixWrapper = result
}
