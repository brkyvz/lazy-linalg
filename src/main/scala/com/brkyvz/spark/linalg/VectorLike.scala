package com.brkyvz.spark.linalg

import org.apache.spark.mllib.linalg.{SparseVector, DenseVector}

trait VectorLike {

  /**
   * Size of the vector.
   */
  def size: Int

  def apply(i: Int): Double

  import VectorOperators._
  def +(y: VectorLike): LazyVector = add(this, y)
  def -(y: VectorLike): LazyVector = sub(this, y)
  def *(y: VectorLike): LazyVector = mul(this, y)
  def /(y: VectorLike): LazyVector = div(this, y)
}

/**
 * A dense vector represented by a value array.
 */
class DenseVectorWrapper(override val values: Array[Double])
  extends DenseVector(values) with VectorLike

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
    override val values: Array[Double]) extends SparseVector(size, indices, values) with VectorLike


trait LazyVector extends VectorLike {
  def compute(into: Option[Array[Double]] = None): VectorLike = {
    val values = into.getOrElse(new Array[Double](size))
    var i = 0
    while (i < size) {
      values(i) = this(i)
      i += 1
    }
    new DenseVectorWrapper(values)
  }
}

abstract class LazyVVOp(
    left: VectorLike,
    right: VectorLike,
    operation: (Double, Double) => Double) extends LazyVector {
  require(left.size == right.size, s"Size of vectors don't match. ${left.size} vs ${right.size}")
  override def size = left.size
}

class LazyDenseVVOp(
    left: VectorLike,
    right: VectorLike,
    operation: (Double, Double) => Double,
    into: Option[DenseVectorWrapper] = None) extends LazyVVOp(left, right, operation) {
  override def apply(i: Int): Double = operation(left(i), right(i))
}

abstract class LazyVOp(parent: VectorLike, operation: Double => Double) extends LazyVector {
  override def size = parent.size
}

class LazyDenseVOp(
    parent: VectorLike,
    operation: Double => Double,
    into: Option[DenseVector] = None) extends LazyVOp(parent, operation) {
  override def apply(i: Int): Double = operation(parent(i))
}
