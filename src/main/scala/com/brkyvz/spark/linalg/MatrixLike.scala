package com.brkyvz.spark.linalg

import org.apache.spark.mllib.linalg.{Matrix, DenseMatrix, SparseMatrix}

trait MatrixLike extends Serializable {

  /** Number of rows. */
  def numRows: Int

  /** Number of columns. */
  def numCols: Int

  def size: Int = numRows * numCols

  def apply(i: Int): Double

  import funcs._
  def +(y: MatrixLike): LazyMatrix = add(this, y)
  def -(y: MatrixLike): LazyMatrix = sub(this, y)
  def :*(y: MatrixLike): LazyMatrix = emul(this, y)
  def *(y: MatrixLike): LazyMatrix
  def /(y: MatrixLike): LazyMatrix = div(this, y)
}

/** Dense and Sparse Matrices can be mutated. Lazy matrices are immutable. */
sealed trait MutableMatrix extends MatrixLike {
  override def *(y: MatrixLike): LazyMatrix = {
    require(this.numCols == y.numRows || y.isInstanceOf[Scalar],
      s"numCols of left side doesn't match numRows of right. ${this.numCols} vs. ${y.numRows}")
    y match {
      case mm: MutableMatrix => new LazyMM_MMultOp(this, mm)
      case lzy: LazyMatrix => new LazyML_MMultOp(this, lzy)
      case scalar: Scalar => funcs.emul(this, scalar)
    }
  }
  def *(y: VectorLike): LazyVector = {
    require(this.numCols == y.size,
      s"numCols of left side doesn't match numRows of right. ${this.numCols} vs. ${y.size}")
    y match {
      case dn: DenseVectorWrapper => new LazyMM_MV_MultOp(this, dn)
      case sp: SparseVectorWrapper => new LazyMM_MV_MultOp(this, sp)
      case lzy: LazyVector => new LazyMM_LV_MultOp(this, lzy)
    }
  }
}

class DenseMatrixWrapper(
    override val numRows: Int,
    override val numCols: Int,
    override val values: Array[Double],
    override val isTransposed: Boolean)
  extends DenseMatrix(numRows, numCols, values, isTransposed) with MutableMatrix {

  def this(numRows: Int, numCols: Int, values: Array[Double]) =
    this(numRows, numCols, values, isTransposed = false)

  override def apply(i: Int): Double = values(i)

  def +=(y: MatrixLike): this.type = {
    require(y.numRows == this.numRows || y.isInstanceOf[Scalar],
      s"Rows don't match for in-place addition. ${this.numRows} vs. ${y.numRows}")
    require(y.numCols == this.numCols || y.isInstanceOf[Scalar],
      s"Cols don't match for in-place addition. ${this.numCols} vs. ${y.numCols}")
    y match {
      case dd: LazyMM_MMultOp =>
        new LazyMM_MMultOp(dd.left, dd.right, Option(this.values), 1.0).compute()
      case dl: LazyML_MMultOp =>
        new LazyML_MMultOp(dl.left, dl.right, Option(this.values), 1.0).compute()
      case ld: LazyLM_MMultOp =>
        new LazyLM_MMultOp(ld.left, ld.right, Option(this.values), 1.0).compute()
      case ll: LazyLL_MMultOp =>
        new LazyLL_MMultOp(ll.left, ll.right, Option(this.values), 1.0).compute()
      case _ => new LazyImDenseMMOp(this, y, _ + _).compute(Option(this.values))
    }
    this
  }

  def :=(y: LazyMatrix): MatrixLike = {
    require(y.numRows == this.numRows,
      s"Rows don't match for in-place evaluation. ${this.numRows} vs. ${y.numRows}")
    require(y.numCols == this.numCols,
      s"Cols don't match for in-place evaluation. ${this.numCols} vs. ${y.numCols}")
    y match {
      case dd: LazyMM_MMultOp =>
        new LazyMM_MMultOp(dd.left, dd.right, Option(this.values), 0.0).compute()
      case dl: LazyML_MMultOp =>
        new LazyML_MMultOp(dl.left, dl.right, Option(this.values), 0.0).compute()
      case ld: LazyLM_MMultOp =>
        new LazyLM_MMultOp(ld.left, ld.right, Option(this.values), 0.0).compute()
      case ll: LazyLL_MMultOp =>
        new LazyLL_MMultOp(ll.left, ll.right, Option(this.values), 0.0).compute()
      case _ => y.compute(Option(this.values))
    }
    this
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

  override def apply(i: Int): Double = this(i % numRows, i / numRows)
}

object SparseMatrixWrapper {
  def apply(mat: SparseMatrix): SparseMatrixWrapper = new SparseMatrixWrapper(mat.numRows,
    mat.numCols, mat.colPtrs, mat.rowIndices, mat.values, mat.isTransposed)
}

sealed trait LazyMatrix extends MatrixLike {
  def compute(into: Option[Array[Double]] = None): MatrixLike = {
    val values = into.getOrElse(new Array[Double](size))
    require(values.length == size,
      s"Size of buffer (${values.length}) not equal to size of matrix ($size).")
    var i = 0
    while (i < size) {
      values(i) = this(i)
      i += 1
    }
    new DenseMatrixWrapper(numRows, numCols, values)
  }
  override def *(y: MatrixLike): LazyMatrix = {
    require(this.numCols == y.numRows || y.isInstanceOf[Scalar],
      s"numCols of left side doesn't match numRows of right. ${this.numCols} vs. ${y.numRows}")
    y match {
      case mm: MutableMatrix => new LazyLM_MMultOp(this, mm)
      case lzy: LazyMatrix => new LazyLL_MMultOp(this, lzy)
      case scalar: Scalar => funcs.emul(this, scalar)
    }
  }
  def *(y: VectorLike): LazyVector = {
    require(this.numCols == y.size,
      s"numCols of left side doesn't match numRows of right. ${this.numCols} vs. ${y.size}")
    y match {
      case dn: DenseVectorWrapper => new LazyLM_MV_MultOp(this, dn)
      case sp: SparseVectorWrapper => new LazyLM_MV_MultOp(this, sp)
      case lzy: LazyVector => new LazyLM_LV_MultOp(this, lzy)
    }
  }
}

private[linalg] abstract class LazyMMOp(
    left: MatrixLike,
    right: MatrixLike,
    operation: (Double, Double) => Double) extends LazyMatrix {
  require(left.numRows == right.numRows || left.isInstanceOf[Scalar] || right.isInstanceOf[Scalar],
    s"Rows don't match for in-place addition. ${left.numRows} vs. ${right.numRows}")
  require(left.numCols == right.numCols || left.isInstanceOf[Scalar] || right.isInstanceOf[Scalar],
    s"Cols don't match for in-place addition. ${left.numCols} vs. ${right.numCols}")
  override def numRows = math.max(left.numRows, right.numRows)
  override def numCols = math.max(left.numCols, right.numCols)
}

private[linalg] class LazyImDenseMMOp(
    left: MatrixLike,
    right: MatrixLike,
    operation: (Double, Double) => Double) extends LazyMMOp(left, right, operation) {
  override def apply(i: Int): Double = operation(left(i), right(i))
}

private[linalg] case class LazyImDenseScaleOp(
    left: Scalar,
    right: MatrixLike) extends LazyImDenseMMOp(left, right, _ * _)

private[linalg] class LazyMatrixMapOp(
    parent: MatrixLike,
    operation: Double => Double) extends LazyMatrix {
  override def numRows = parent.numRows
  override def numCols = parent.numCols
  override def apply(i: Int): Double = operation(parent(i))
}

private[linalg] abstract class LazyMMultOp(
    left: MatrixLike,
    right: MatrixLike,
    into: Option[Array[Double]] = None,
    beta: Double = 1.0) extends LazyMatrix {
  override def numRows = left.numRows
  override def numCols = right.numCols
}

private[linalg] class LazyLL_MMultOp(
    val left: LazyMatrix,
    val right: LazyMatrix,
    into: Option[Array[Double]] = None,
    beta: Double = 1.0) extends LazyMMultOp(left, right, into, beta) {
  override def apply(i: Int): Double = result(i)

  private var buffer: Option[Array[Double]] = into

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
      if (leftRes.isEmpty && rightRes.isEmpty) {
        val inside = new DenseMatrixWrapper(effLeft.numRows, effRight.numCols,
          buffer.getOrElse(new Array[Double](effLeft.numRows * effRight.numCols)))
        BLASUtils.gemm(leftScale * rightScale, effLeft, effRight, beta, inside)
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
          case r: LazyMatrix => new LazyLL_MMultOp(l, r, buffer, beta).compute()
          case d: DenseMatrixWrapper => new LazyLM_MMultOp(l, d, buffer, beta).compute()
        }
      case ld: DenseMatrixWrapper =>
        rebuildRight match {
          case r: LazyMatrix => new LazyML_MMultOp(ld, r, buffer, beta).compute()
          case d: DenseMatrixWrapper => new LazyMM_MMultOp(ld, d, buffer, beta).compute()
        }
      case None =>
        rebuildRight match {
          case r: LazyMM_MMultOp => new LazyMM_MMultOp(r.left, r.right, buffer, beta).compute()
          case l: LazyML_MMultOp => new LazyML_MMultOp(l.left, l.right, buffer, beta).compute()
          case d: DenseMatrixWrapper => d
        }
    }
  }
  override def compute(into: Option[Array[Double]] = None): DenseMatrixWrapper = {
    into.foreach(b => buffer = Option(b))
    result
  }
}

private[linalg] class LazyLM_MMultOp(
    val left: LazyMatrix,
    val right: MutableMatrix,
    into: Option[Array[Double]] = None,
    beta: Double = 1.0) extends LazyMMultOp(left, right, into, beta) {
  override def apply(i: Int): Double = result(i)

  private var buffer: Option[Array[Double]] = into

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
      if (leftRes.isEmpty) {
        val inside = new DenseMatrixWrapper(effLeft.numRows, right.numCols,
          buffer.getOrElse(new Array[Double](effLeft.numRows * right.numCols)))
        BLASUtils.gemm(leftScale, effLeft, right, beta, inside)
        inside
      } else {
        val inside = DenseMatrix.zeros(effLeft.numRows, right.numCols)
        BLASUtils.gemm(leftScale, effLeft, right, 1.0, inside)
        inside
      }

    leftRes.getOrElse(None) match {
      case l: LazyMatrix => new LazyLM_MMultOp(l, middle, buffer, beta).compute()
      case ld: DenseMatrixWrapper => new LazyMM_MMultOp(ld, middle, buffer, beta).compute()
      case None => middle
    }
  }

  override def compute(into: Option[Array[Double]] = None): DenseMatrixWrapper = {
    into.foreach(b => buffer = Option(b))
    result
  }
}

private[linalg] class LazyML_MMultOp(
    val left: MutableMatrix,
    val right: LazyMatrix,
    into: Option[Array[Double]] = None,
    beta: Double = 1.0) extends LazyMMultOp(left, right, into, beta) {
  override def apply(i: Int): Double = result(i)

  private var buffer: Option[Array[Double]] = into

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
      if (rightRes.isEmpty) {
        val inside = new DenseMatrixWrapper(left.numRows, effRight.numCols,
          buffer.getOrElse(new Array[Double](left.numRows * effRight.numCols)))
        BLASUtils.gemm(rightScale, left, effRight, beta, inside)
        inside
      } else {
        val inside = DenseMatrix.zeros(left.numRows, effRight.numCols)
        BLASUtils.gemm(rightScale, left, effRight, 0.0, inside)
        inside
      }

    rightRes.getOrElse(None) match {
      case l: LazyMatrix => new LazyML_MMultOp(middle, l, buffer, beta).compute()
      case d: DenseMatrixWrapper => new LazyMM_MMultOp(middle, d, buffer, beta).compute()
      case None => middle
    }
  }

  override def compute(into: Option[Array[Double]] = None): DenseMatrixWrapper = {
    into.foreach(b => buffer = Option(b))
    result
  }
}

private[linalg] class LazyMM_MMultOp(
    val left: MutableMatrix,
    val right: MutableMatrix,
    into: Option[Array[Double]] = None,
    beta: Double = 1.0) extends LazyMMultOp(left, right, into, beta) {
  override def apply(i: Int): Double = result(i)

  private var buffer: Option[Array[Double]] = into

  lazy val result: DenseMatrixWrapper = {
    val inside = new DenseMatrixWrapper(left.numRows, right.numCols,
      buffer.getOrElse(new Array[Double](left.numRows * right.numCols)))
    BLASUtils.gemm(1.0, left, right, beta, inside)
    inside
  }

  override def compute(into: Option[Array[Double]] = None): DenseMatrixWrapper = {
    into.foreach(b => buffer = Option(b))
    result
  }
}
