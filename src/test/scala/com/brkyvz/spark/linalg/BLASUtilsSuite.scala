package com.brkyvz.spark.linalg

import BLASUtils._
import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg._

import com.brkyvz.spark.util.TestingUtils._

class BLASUtilsSuite extends FunSuite {

  test("scal") {
    val a = 0.1
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)

    scal(a, sx)
    assert(sx ~== Vectors.sparse(3, Array(0, 2), Array(0.1, -0.2)) absTol 1e-15)

    scal(a, dx)
    assert(dx ~== Vectors.dense(0.1, 0.0, -0.2) absTol 1e-15)
  }

  test("axpy") {
    val alpha = 0.1
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)
    val dy = Array(2.0, 1.0, 0.0)
    val expected = Vectors.dense(2.1, 1.0, -0.2)

    val dy1 = Vectors.dense(dy.clone())
    axpy(alpha, sx, dy1)
    assert(dy1 ~== expected absTol 1e-15)

    val dy2 = Vectors.dense(dy.clone())
    axpy(alpha, dx, dy2)
    assert(dy2 ~== expected absTol 1e-15)

    val sy = Vectors.sparse(4, Array(0, 1), Array(2.0, 1.0))

    intercept[IllegalArgumentException] {
      axpy(alpha, sx, sy)
    }

    intercept[IllegalArgumentException] {
      axpy(alpha, dx, sy)
    }

    withClue("vector sizes must match") {
      intercept[Exception] {
        axpy(alpha, sx, Vectors.dense(1.0, 2.0))
      }
    }
  }

  test("dot") {
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)
    val sy = Vectors.sparse(3, Array(0, 1), Array(2.0, 1.0))
    val dy = Vectors.dense(2.0, 1.0, 0.0)

    assert(dot(sx, sy) ~== 2.0 absTol 1e-15)
    assert(dot(sy, sx) ~== 2.0 absTol 1e-15)
    assert(dot(sx, dy) ~== 2.0 absTol 1e-15)
    assert(dot(dy, sx) ~== 2.0 absTol 1e-15)
    assert(dot(dx, dy) ~== 2.0 absTol 1e-15)
    assert(dot(dy, dx) ~== 2.0 absTol 1e-15)

    assert(dot(sx, sx) ~== 5.0 absTol 1e-15)
    assert(dot(dx, dx) ~== 5.0 absTol 1e-15)
    assert(dot(sx, dx) ~== 5.0 absTol 1e-15)
    assert(dot(dx, sx) ~== 5.0 absTol 1e-15)

    val sx1 = Vectors.sparse(10, Array(0, 3, 5, 7, 8), Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val sx2 = Vectors.sparse(10, Array(1, 3, 6, 7, 9), Array(1.0, 2.0, 3.0, 4.0, 5.0))
    assert(dot(sx1, sx2) ~== 20.0 absTol 1e-15)
    assert(dot(sx2, sx1) ~== 20.0 absTol 1e-15)

    withClue("vector sizes must match") {
      intercept[Exception] {
        dot(sx, Vectors.dense(2.0, 1.0))
      }
    }
  }

  test("syr") {
    val dA = new DenseMatrix(4, 4,
      Array(0.0, 1.2, 2.2, 3.1, 1.2, 3.2, 5.3, 4.6, 2.2, 5.3, 1.8, 3.0, 3.1, 4.6, 3.0, 0.8))
    val x = new DenseVector(Array(0.0, 2.7, 3.5, 2.1))
    val alpha = 0.15

    val expected = new DenseMatrix(4, 4,
      Array(0.0, 1.2, 2.2, 3.1, 1.2, 4.2935, 6.7175, 5.4505, 2.2, 6.7175, 3.6375, 4.1025, 3.1,
        5.4505, 4.1025, 1.4615))

    syr(alpha, x, dA)

    assert(dA ~== expected absTol 1e-15)

    val dB =
      new DenseMatrix(3, 4, Array(0.0, 1.2, 2.2, 3.1, 1.2, 3.2, 5.3, 4.6, 2.2, 5.3, 1.8, 3.0))

    withClue("Matrix A must be a symmetric Matrix") {
      intercept[Exception] {
        syr(alpha, x, dB)
      }
    }

    val dC =
      new DenseMatrix(3, 3, Array(0.0, 1.2, 2.2, 1.2, 3.2, 5.3, 2.2, 5.3, 1.8))

    withClue("Size of vector must match the rank of matrix") {
      intercept[Exception] {
        syr(alpha, x, dC)
      }
    }

    val y = new DenseVector(Array(0.0, 2.7, 3.5, 2.1, 1.5))

    withClue("Size of vector must match the rank of matrix") {
      intercept[Exception] {
        syr(alpha, y, dA)
      }
    }

    val xSparse = new SparseVector(4, Array(0, 2, 3), Array(1.0, 3.0, 4.0))
    val dD = new DenseMatrix(4, 4,
      Array(0.0, 1.2, 2.2, 3.1, 1.2, 3.2, 5.3, 4.6, 2.2, 5.3, 1.8, 3.0, 3.1, 4.6, 3.0, 0.8))
    syr(0.1, xSparse, dD)
    val expectedSparse = new DenseMatrix(4, 4,
      Array(0.1, 1.2, 2.5, 3.5, 1.2, 3.2, 5.3, 4.6, 2.5, 5.3, 2.7, 4.2, 3.5, 4.6, 4.2, 2.4))
    assert(dD ~== expectedSparse absTol 1e-15)
  }

  test("gemm") {
    val dA =
      new DenseMatrix(4, 3, Array(0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 3.0))
    val sA = new SparseMatrix(4, 3, Array(0, 1, 3, 4), Array(1, 0, 2, 3), Array(1.0, 2.0, 1.0, 3.0))

    val dB = new DenseMatrix(3, 2, Array(1.0, 0.0, 0.0, 0.0, 2.0, 1.0))
    val sB = new SparseMatrix(3, 2, Array(0, 1, 3), Array(0, 1, 2), Array(1.0, 2.0, 1.0))
    val expected = new DenseMatrix(4, 2, Array(0.0, 1.0, 0.0, 0.0, 4.0, 0.0, 2.0, 3.0))
    val dBTman = new DenseMatrix(2, 3, Array(1.0, 0.0, 0.0, 2.0, 0.0, 1.0))
    val sBTman = new SparseMatrix(2, 3, Array(0, 1, 2, 3), Array(0, 1, 1), Array(1.0, 2.0, 1.0))

    assert(dA.multiply(dB) ~== expected absTol 1e-15)
    assert(sA.multiply(dB) ~== expected absTol 1e-15)

    val C1 = new DenseMatrix(4, 2, Array(1.0, 0.0, 2.0, 1.0, 0.0, 0.0, 1.0, 0.0))
    val C2 = C1.copy
    val expected2 = new DenseMatrix(4, 2, Array(2.0, 1.0, 4.0, 2.0, 4.0, 0.0, 4.0, 3.0))
    val expected3 = new DenseMatrix(4, 2, Array(2.0, 2.0, 4.0, 2.0, 8.0, 0.0, 6.0, 6.0))
    val expected4 = new DenseMatrix(4, 2, Array(5.0, 0.0, 10.0, 5.0, 0.0, 0.0, 5.0, 0.0))
    val expected5 = new DenseMatrix(4, 2, Array(1.0, 0.0, 2.0, 1.0, 0.0, 0.0, 1.0, 0.0))

    gemm(1.0, dA, dB, 0.0, C2)
    assert(C2 ~== expected absTol 1e-15)
    gemm(1.0, sA, dB, 0.0, C2)
    assert(C2 ~== expected absTol 1e-15)

    withClue("columns of A don't match the rows of B") {
      intercept[Exception] {
        gemm(1.0, dA.transpose, dB, 2.0, C1)
      }
    }

    val dATman =
      new DenseMatrix(3, 4, Array(0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 3.0))
    val sATman =
      new SparseMatrix(3, 4, Array(0, 1, 2, 3, 4), Array(1, 0, 1, 2), Array(2.0, 1.0, 1.0, 3.0))

    val dATT = dATman.transpose
    val sATT = sATman.transpose
    val BTT = dBTman.transpose
    val sBTT = dBTman.toSparse.transpose

    val combinations = Seq((1.0, 0.0, expected), (1.0, 2.0, expected2), (2.0, 2.0, expected3),
      (0.0, 5.0, expected4), (0.0, 1.0, expected5))

    combinations.foreach { case (alpha, beta, expectation) =>
      def checkResult(a: MatrixLike, b: MatrixLike): Unit = {
        val Cres = C1.copy
        gemm(alpha, a, b, beta, Cres)
        assert(Cres ~== expectation absTol 1e-15)
      }
      checkResult(dA, dB)
      checkResult(dA, sB)
      checkResult(dA, BTT)
      checkResult(dA, sBTT)
      checkResult(sA, dB)
      checkResult(sA, sB)
      checkResult(sA, BTT)
      checkResult(sA, sBTT)
      checkResult(dATT, dB)
      checkResult(dATT, BTT)
      checkResult(dATT, sB)
      checkResult(dATT, sBTT)
      checkResult(sATT, dB)
      checkResult(sATT, BTT)
      checkResult(sATT, sB)
      checkResult(sATT, sBTT)
    }
  }

  test("gemv") {
    val dA =
      new DenseMatrix(4, 3, Array(0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 3.0))
    val sA = new SparseMatrix(4, 3, Array(0, 1, 3, 4), Array(1, 0, 2, 3), Array(1.0, 2.0, 1.0, 3.0))

    val dx = new DenseVector(Array(1.0, 2.0, 3.0))
    val sx = dx.toSparse
    val expected = new DenseVector(Array(4.0, 1.0, 2.0, 9.0))

    assert(dA.multiply(dx) ~== expected absTol 1e-15)
    assert(sA.multiply(dx) ~== expected absTol 1e-15)
    assert(dA.multiply(sx) ~== expected absTol 1e-15)
    assert(sA.multiply(sx) ~== expected absTol 1e-15)

    val y1 = new DenseVector(Array(1.0, 3.0, 1.0, 0.0))

    val expected2 = new DenseVector(Array(6.0, 7.0, 4.0, 9.0))
    val expected3 = new DenseVector(Array(10.0, 8.0, 6.0, 18.0))

    withClue("columns of A don't match the rows of B") {
      intercept[Exception] {
        gemv(1.0, dA.transpose, dx, 2.0, y1)
      }
      intercept[Exception] {
        gemv(1.0, sA.transpose, dx, 2.0, y1)
      }
      intercept[Exception] {
        gemv(1.0, dA.transpose, sx, 2.0, y1)
      }
      intercept[Exception] {
        gemv(1.0, sA.transpose, sx, 2.0, y1)
      }
    }

    val dAT =
      new DenseMatrix(3, 4, Array(0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 3.0))
    val sAT =
      new SparseMatrix(3, 4, Array(0, 1, 2, 3, 4), Array(1, 0, 1, 2), Array(2.0, 1.0, 1.0, 3.0))

    val dATT = dAT.transpose
    val sATT = sAT.transpose

    val combinations = Seq((1.0, 0.0, expected), (1.0, 2.0, expected2), (2.0, 2.0, expected3))

    combinations.foreach { case (alpha, beta, expectation) =>
      def checkResult(a: MatrixLike, b: VectorLike): Unit = {
        val Yres = y1.copy
        gemv(alpha, a, b, beta, Yres)
        assert(Yres ~== expectation absTol 1e-15)
      }
      checkResult(dA, dx)
      checkResult(dA, sx)
      checkResult(sA, dx)
      checkResult(sA, sx)
      checkResult(dATT, dx)
      checkResult(dATT, sx)
      checkResult(sATT, dx)
      checkResult(sATT, sx)
    }
  }
}
