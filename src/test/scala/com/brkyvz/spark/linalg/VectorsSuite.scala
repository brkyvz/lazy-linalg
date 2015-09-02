package com.brkyvz.spark.linalg

import java.util.Random

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{DenseMatrix, DenseVector, Vectors}

class VectorsSuite extends FunSuite with SharedSparkContext {

  private val a = Vectors.dense(1, 2, 3, 4)
  private val b = new DenseVector(Array(0, -2, 0, -2))
  private val c = Vectors.sparse(4, Seq((0, 1.0)))
  private val x = Vectors.sparse(5, Seq((3, 0.5)))

  test("basic arithmetic") {
    val buffer = new Array[Double](4)
    val wrapper = new DenseVector(buffer)

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
    val sparseBuffer = new Array[Double](1)
    (x * 3).compute(Option(sparseBuffer))
    assert(sparseBuffer.toSeq === Seq(1.5))
  }

  test("sparse ops remain sparse") {
    val d = Vectors.sparse(4, Seq((1, 1.0), (3, 2.0)))
    val res = (c + d).compute()
    assert(res(0) === 1.0)
    assert(res(1) === 1.0)
    assert(res(2) === 0.0)
    assert(res(3) === 2.0)
    val sparse = res.asInstanceOf[SparseVectorWrapper]
    assert(sparse.values.length === 3)
    assert(sparse.indices.length === 3)
    assert(sparse.indices.toSeq === Seq(0, 1, 3))
    assert(sparse.size === 4)
  }

  test("funcs") {
    import funcs._
    val buffer = new Array[Double](4)
    val buffer2 = new Array[Double](5)
    pow(a, c).compute(Option(buffer))
    assert(buffer.toSeq === Seq(1.0, 1.0, 1.0, 1.0))
    val sparseBuffer = new Array[Double](1)
    exp(x).compute(Option(sparseBuffer))
    assert(sparseBuffer.toSeq === Seq(java.lang.Math.exp(0.5)))
    exp(x).compute(Option(buffer2))
    assert(buffer2.toSeq === Seq(1.0, 1.0, 1.0, java.lang.Math.exp(0.5), 1.0))
    apply(a, c, (m: Double, n: Double) => m + n).compute(Option(buffer))
    assert(buffer.toSeq === Seq(2.0, 2.0, 3.0, 4.0))
  }

  test("blas methods") {
    var d = new DenseVectorWrapper(a.copy.toArray)
    d += a * 3
    val e = (a * 4).compute()
    assert(d.asInstanceOf[DenseVector].values.toSeq === e.asInstanceOf[DenseVector].values.toSeq)

    val A = DenseMatrix.rand(5, 4, new Random())
    val resSpark = A.multiply(a)
    val buffer = new Array[Double](5)
    val res = (A * a).compute(Option(buffer))
    assert(resSpark.values.toSeq === buffer.toSeq)
  }

  test("rdd methods") {
    val rdd = sc.parallelize(Seq(a, b, c))
    val Array(res1, res2, res3) =
      rdd.map(v => (v + 2).compute().asInstanceOf[DenseVector]).collect()
    assert(res1.values.toSeq === Seq(3.0, 4.0, 5.0, 6.0))
    assert(res2.values.toSeq === Seq(2.0, 0.0, 2.0, 0.0))
    assert(res3.values.toSeq === Seq(3.0, 2.0, 2.0, 2.0))
    val Array(res4, res5, res6) = rdd.map(v => v + 2).map(_ - 1).collect()
    assert(res4.compute().asInstanceOf[DenseVector].values.toSeq === Seq(2.0, 3.0, 4.0, 5.0))
    assert(res5.compute().asInstanceOf[DenseVector].values.toSeq === Seq(1.0, -1.0, 1.0, -1.0))
    assert(res6.compute().asInstanceOf[DenseVector].values.toSeq === Seq(2.0, 1.0, 1.0, 1.0))

    val sum = rdd.aggregate(new DenseVector(Array(0, 0, 0, 0)))(
      seqOp = (base, element) => base += element,
      combOp = (base1, base2) => base1 += base2
    )
    assert(sum.values.toSeq === Seq(2.0, 0.0, 3.0, 2.0))
    val sum2 = rdd.aggregate(new DenseVector(Array(0, 0, 0, 0)))(
      seqOp = (base, element) => base += element * 2 - 1,
      combOp = (base1, base2) => base1 += base2
    )
    assert(sum2.values.toSeq === Seq(1.0, -3.0, 3.0, 1.0))
  }
}
