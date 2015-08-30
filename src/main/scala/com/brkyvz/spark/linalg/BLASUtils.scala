package com.brkyvz.spark.linalg

import java.lang.reflect.InvocationTargetException
import java.lang.{Double => JavaDouble}

import org.apache.spark.mllib.linalg._

/** Util methods that use reflection to call into MLlib's private BLAS methods. */
object BLASUtils {

  @transient private lazy val clazz: Class[_] = Class.forName("org.apache.spark.mllib.linalg.BLAS$")

  @transient private lazy val _blas: Any = {
    val constructor = clazz.getDeclaredConstructors.head
    constructor.setAccessible(true)
    constructor.newInstance()
  }

  private def castMatrix(mat: MatrixLike, toDense: Boolean = false): Matrix = mat match {
    case dn: DenseMatrixWrapper => dn.asInstanceOf[DenseMatrix]
    case sp: SparseMatrixWrapper =>
      if (toDense) sp.toDense else sp.asInstanceOf[SparseMatrix]
    case lzy: LazyMatrix => lzy.compute().asInstanceOf[DenseMatrix]
    case _ => throw new UnsupportedOperationException(s"${mat.getClass} can't be cast to Matrix.")
  }

  private def castVector(mat: VectorLike, toDense: Boolean = false): Vector = mat match {
    case dn: DenseVectorWrapper => dn.asInstanceOf[DenseVector]
    case sp: SparseVectorWrapper =>
      if (toDense) sp.toDense else sp.asInstanceOf[SparseVector]
    case lzy: LazyVector => lzy.compute().asInstanceOf[DenseVector]
    case _ => throw new UnsupportedOperationException(s"${mat.getClass} can't be cast to Vector.")
  }

  private def invokeMethod(methodName: String, args: (Class[_], AnyRef)*): Any = {
    val (types, values) = args.unzip
    val method = clazz.getDeclaredMethod(methodName, types: _*)
    method.setAccessible(true)
    try {
      method.invoke(_blas, values.toSeq: _*)
    } catch {
      case ex: InvocationTargetException =>
        throw new IllegalArgumentException(s"$methodName is not supported for arguments: $values")
    }
  }

  /**
   * y += a * x
   */
  def axpy(a: Double, x: Vector, y: Vector): Unit = {
    val args: Seq[(Class[_], AnyRef)] = Seq((classOf[Double], new JavaDouble(a)),
      (classOf[Vector], castVector(x)), (classOf[Vector], castVector(y)))
    invokeMethod("axpy", args: _*)
  }

  /**
   * x^T^y
   */
  def dot(x: VectorLike, y: VectorLike): Double = {
    val args: Seq[(Class[_], AnyRef)] = Seq(
      (classOf[Vector], castVector(x)), (classOf[Vector], castVector(y)))
    invokeMethod("dot", args: _*).asInstanceOf[Double]
  }

  /**
   * x = a * x
   */
  def scal(a: Double, x: VectorLike): Unit = {
    val cx = castVector(x)
    val args: Seq[(Class[_], AnyRef)] = Seq(
      (classOf[Double], new JavaDouble(a)), (classOf[Vector], cx))
    invokeMethod("scal", args: _*)
  }

  /**
   * A := alpha * x * x^T^ + A
   * @param alpha a real scalar that will be multiplied to x * x^T^.
   * @param x the vector x that contains the n elements.
   * @param A the symmetric matrix A. Size of n x n.
   */
  def syr(alpha: Double, x: Vector, A: MatrixLike): Unit = {
    val args: Seq[(Class[_], AnyRef)] = Seq((classOf[Double], new JavaDouble(alpha)),
      (classOf[Vector], castVector(x)), (classOf[DenseMatrix], castMatrix(A, toDense = true)))
    invokeMethod("syr", args: _*)
  }

  def gemm(alpha: Double, a: MatrixLike, b: MatrixLike, beta: Double, c: DenseMatrix): Unit = {
    b match {
      case dnB: DenseMatrixWrapper => mllibGemm(alpha, castMatrix(a), dnB, beta, c)
      case spB: SparseMatrixWrapper =>
        a match {
          case dnA: DenseMatrixWrapper => dsgemm(alpha, dnA, spB, beta, c)
          case spA: SparseMatrixWrapper => mllibGemm(alpha, spA, spB.toDense, beta, c)
          case lzy: LazyMatrix =>
            dsgemm(alpha, lzy.compute().asInstanceOf[DenseMatrixWrapper], spB, beta, c)
        }
      case lzy: LazyMatrix =>
        mllibGemm(alpha, castMatrix(a), lzy.compute().asInstanceOf[DenseMatrix], beta, c)
    }
  }

  private def mllibGemm(
      alpha: Double,
      A: Matrix,
      B: DenseMatrix,
      beta: Double,
      C: DenseMatrix): Unit = {
    val args: Seq[(Class[_], AnyRef)] = Seq(
      (classOf[Double], new JavaDouble(alpha)), (classOf[Matrix], A), (classOf[DenseMatrix], B),
      (classOf[Double], new JavaDouble(beta)), (classOf[DenseMatrix], C))
    invokeMethod("gemm", args: _*)
  }

  private def dsgemm(
      alpha: Double,
      A: DenseMatrixWrapper,
      B: SparseMatrixWrapper,
      beta: Double,
      C: DenseMatrix): Unit = {
    val mA: Int = A.numRows
    val nB: Int = B.numCols
    val kA: Int = A.numCols
    val kB: Int = B.numRows

    require(kA == kB, s"The columns of A don't match the rows of B. A: $kA, B: $kB")
    require(mA == C.numRows, s"The rows of C don't match the rows of A. C: ${C.numRows}, A: $mA")
    require(nB == C.numCols,
      s"The columns of C don't match the columns of B. C: ${C.numCols}, A: $nB")

    val Avals = A.values
    val Bvals = B.values
    val Cvals = C.values
    val BrowIndices = B.rowIndices
    val BcolPtrs = B.colPtrs

    // Slicing is easy in this case. This is the optimal multiplication setting for sparse matrices
    if (!B.isTransposed){
      var colCounterForB = 0
      if (A.isTransposed) { // Expensive to put the check inside the loop
        while (colCounterForB < nB) {
          var rowCounterForA = 0
          val Cstart = colCounterForB * mA
          val Bstart = BcolPtrs(colCounterForB)
          while (rowCounterForA < mA) {
            var i = Bstart
            val indEnd = BcolPtrs(colCounterForB + 1)
            val Astart = rowCounterForA * kA
            var sum = 0.0
            while (i < indEnd) {
              sum += Avals(Astart + BrowIndices(i)) * Bvals(i)
              i += 1
            }
            val Cindex = Cstart + rowCounterForA
            Cvals(Cindex) = beta * Cvals(Cindex) + sum * alpha
            rowCounterForA += 1
          }
          colCounterForB += 1
        }
      } else {
        while (colCounterForB < nB) {
          var rowCounterForA = 0
          val Cstart = colCounterForB * mA
          while (rowCounterForA < mA) {
            var i = BcolPtrs(colCounterForB)
            val indEnd = BcolPtrs(colCounterForB + 1)
            var sum = 0.0
            while (i < indEnd) {
              sum += A(rowCounterForA, BrowIndices(i)) * Bvals(i)
              i += 1
            }
            val Cindex = Cstart + rowCounterForA
            Cvals(Cindex) = beta * Cvals(Cindex) + sum * alpha
            rowCounterForA += 1
          }
          colCounterForB += 1
        }
      }
    } else {
      // Scale matrix first if `beta` is not equal to 0.0
      if (beta != 1.0) {
        scal(beta, new DenseVectorWrapper(C.values))
      }
      // Perform matrix multiplication and add to C. The rows of A are multiplied by the columns of
      // B, and added to C.
      var rowCounterForB = 0 // the column to be updated in C
      if (!A.isTransposed) { // Expensive to put the check inside the loop
        while (rowCounterForB < kB) {
          var i = BcolPtrs(rowCounterForB)
          val indEnd = BcolPtrs(rowCounterForB + 1)
          while (i < indEnd) {
            var rowCounterForA = 0 // The column of A to multiply with the row of B
            val Bval = Bvals(i) * alpha
            val Cstart = BrowIndices(i) * mA
            val Astart = rowCounterForB * mA
            while (rowCounterForA < mA) {
              Cvals(Cstart + rowCounterForA) += Avals(Astart + rowCounterForA) * Bval
              rowCounterForA += 1
            }
            i += 1
          }
          rowCounterForB += 1
        }
      } else {
        while (rowCounterForB < kB) {
          var i = BcolPtrs(rowCounterForB)
          val indEnd = BcolPtrs(rowCounterForB + 1)
          while (i < indEnd) {
            var rowCounterForA = 0 // The column of A to multiply with the row of B
            val Bval = Bvals(i) * alpha
            val Bcol = BrowIndices(i)
            val Cstart = Bcol * mA
            while (rowCounterForA < mA) {
              Cvals(Cstart + rowCounterForA) += A(rowCounterForA, rowCounterForB) * Bval
              rowCounterForA += 1
            }
            i += 1
          }
          rowCounterForB += 1
        }
      }
    }
  }

  def gemv(
      alpha: Double,
      a: MatrixLike,
      x: VectorLike,
      beta: Double,
      y: VectorLike): Unit = {
    val A: Matrix = castMatrix(a)
    val _x: Vector = castVector(x)
    val _y: Vector = castVector(y)
    val args: Seq[(Class[_], AnyRef)] = Seq((classOf[Double], new JavaDouble(alpha)),
      (classOf[Matrix], A), (classOf[Vector], x),
      (classOf[Double], new JavaDouble(beta)), (classOf[DenseVector], y))
    invokeMethod("gemv", args: _*)
  }
}
