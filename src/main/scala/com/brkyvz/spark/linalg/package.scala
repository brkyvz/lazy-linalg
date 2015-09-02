package com.brkyvz.spark

import scala.language.implicitConversions

import org.apache.spark.mllib.linalg._

package object linalg {

  implicit def wrapDenseMatrix(x: DenseMatrix): DenseMatrixWrapper = DenseMatrixWrapper(x)
  implicit def wrapSparseMatrix(x: SparseMatrix): SparseMatrixWrapper = SparseMatrixWrapper(x)

  implicit def wrapMatrix(x: Matrix): MatrixLike = x match {
    case dn: DenseMatrix => DenseMatrixWrapper(dn)
    case sp: SparseMatrix => SparseMatrixWrapper(sp)
  }

  implicit def wrapDenseVector(x: DenseVector): DenseVectorWrapper =
    new DenseVectorWrapper(x.values)

  implicit def wrapSparseVector(x: SparseVector): SparseVectorWrapper =
    new SparseVectorWrapper(x.size, x.indices, x.values)

  implicit def wrapVector(x: Vector): VectorLike = x match {
    case dn: DenseVector => new DenseVectorWrapper(dn.values)
    case sp: SparseVector => new SparseVectorWrapper(sp.size, sp.indices, sp.values)
  }

  trait Scalar extends MatrixLike {
    val value: Double

    def *(y: MatrixLike): LazyMatrix = LazyImDenseScaleOp(this, y)
    def *(y: VectorLike): LazyVector = LazyVectorScaleOp(this, y)
  }

  implicit def double2Scalar(x: Double): Scalar = new Scalar {
    override val value: Double = x
    def apply(i: Int): Double = value
    override def numRows = 1
    override def numCols = 1
  }

  implicit def int2Scalar(x: Int): Scalar = new Scalar {
    override val value: Double = x.toDouble
    def apply(i: Int): Double = value
    override def numRows = 1
    override def numCols = 1
  }

  implicit def float2Scalar(x: Float): Scalar = new Scalar {
    override val value: Double = x.toDouble
    def apply(i: Int): Double = value
    override def numRows = 1
    override def numCols = 1
  }

  implicit def long2Scalar(x: Long): Scalar = new Scalar {
    override val value: Double = x.toDouble
    def apply(i: Int): Double = value
    override def numRows = 1
    override def numCols = 1
  }
}
