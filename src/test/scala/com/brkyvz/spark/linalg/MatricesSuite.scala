package com.brkyvz.spark.linalg

import com.holdenkarau.spark.testing.PerTestSparkContext
import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices}

class MatricesSuite extends FunSuite with PerTestSparkContext {

  private val a = Matrices.dense(2, 2, Array(1, 2, 3, 4))
  private val b = new DenseMatrix(2, 2, Array(0, -2, 0, -2))
  private val c = Matrices.sparse(2, 2, Array(0, 1, 1), Array(0), Array(1.0))
  private val x = Matrices.sparse(3, 2, Array(0, 1, 2), Array(0, 2), Array(0.5, 2.0))

  test("basic arithmetic") {
    val buffer = new Array[Double](4)
    val wrapper = new DenseMatrix(2, 2, buffer)

    wrapper := a + b
    assert(wrapper.values.toSeq === Seq(1.0, 0.0, 3.0, 2.0))
    assert(buffer.toSeq === Seq(1.0, 0.0, 3.0, 2.0))

    val buffer2 = new Array[Double](4)
    (a + b).compute(Option(buffer2))
    assert(buffer2.toSeq === Seq(1.0, 0.0, 3.0, 2.0))

    wrapper := a * 2
    assert(wrapper.values.toSeq === Seq(2.0, 4.0, 6.0, 8.0))

    wrapper := a - c
    assert(wrapper.values.toSeq === Seq(0.0, 2.0, 3.0, 4.0))
  }

  test("requires right buffer size") {
    val wrongSizedBuffer = new Array[Double](5)
    intercept[IllegalArgumentException]((a + b).compute(Option(wrongSizedBuffer)))
  }

  test("size mismatch throws error") {
    intercept[IllegalArgumentException]((a + x).compute())
  }

  test("scalar op") {
    val buffer = new Array[Double](4)
    (a + 2).compute(Option(buffer))
    assert(buffer.toSeq === Seq(3.0, 4.0, 5.0, 6.0))
    (c + 2).compute(Option(buffer))
    assert(buffer.toSeq === Seq(3.0, 2.0, 2.0, 2.0))
    val sparseBuffer = new Array[Double](6)
    (x * 3).compute(Option(sparseBuffer))
    assert(sparseBuffer.toSeq === Seq(1.5, 0.0, 0.0, 0.0, 0.0, 6.0))
  }

  test("funcs") {
    import com.brkyvz.spark.linalg.funcs._
    val buffer = new Array[Double](4)
    val buffer2 = new Array[Double](6)
    pow(a, c).compute(Option(buffer))
    assert(buffer.toSeq === Seq(1.0, 1.0, 1.0, 1.0))
    val sparseBuffer = new Array[Double](6)
    exp(x).compute(Option(sparseBuffer))
    assert(sparseBuffer.toSeq ===
      Seq(java.lang.Math.exp(0.5), 1.0, 1.0, 1.0, 1.0, java.lang.Math.exp(2.0)))
    apply(a, c, (m: Double, n: Double) => m + n).compute(Option(buffer))
    assert(buffer.toSeq === Seq(2.0, 2.0, 3.0, 4.0))
  }

  test("blas methods") {
    var d = new DenseMatrixWrapper(2, 2, a.copy.toArray)
    d += a * 3
    val e = (a * 4).compute()
    assert(d.asInstanceOf[DenseMatrix].values.toSeq === e.asInstanceOf[DenseMatrix].values.toSeq)

    val A = DenseMatrix.eye(2)
    A += c * a
    assert(A.values.toSeq === Seq(2.0, 0.0, 3.0, 1.0))
  }

  test("rdd methods") {
    val rdd = sc.parallelize(Seq(a, b, c))
    val Array(res1, res2, res3) =
      rdd.map(v => (v + 2).compute().asInstanceOf[DenseMatrix]).collect()
    assert(res1.values.toSeq === Seq(3.0, 4.0, 5.0, 6.0))
    assert(res2.values.toSeq === Seq(2.0, 0.0, 2.0, 0.0))
    assert(res3.values.toSeq === Seq(3.0, 2.0, 2.0, 2.0))
    val Array(res4, res5, res6) = rdd.map(v => v + 2).map(_ - 1).collect()
    assert(res4.compute().asInstanceOf[DenseMatrix].values.toSeq === Seq(2.0, 3.0, 4.0, 5.0))
    assert(res5.compute().asInstanceOf[DenseMatrix].values.toSeq === Seq(1.0, -1.0, 1.0, -1.0))
    assert(res6.compute().asInstanceOf[DenseMatrix].values.toSeq === Seq(2.0, 1.0, 1.0, 1.0))

    val sum = rdd.aggregate(DenseMatrix.zeros(2, 2))(
      seqOp = (base, element) => base += element,
      combOp = (base1, base2) => base1 += base2
    )
    assert(sum.values.toSeq === Seq(2.0, 0.0, 3.0, 2.0))
    val sum2 = rdd.aggregate(DenseMatrix.zeros(2, 2))(
      seqOp = (base, element) => base += element * 2 - 1,
      combOp = (base1, base2) => base1 += base2
    )
    assert(sum2.values.toSeq === Seq(1.0, -3.0, 3.0, 1.0))
  }
}
