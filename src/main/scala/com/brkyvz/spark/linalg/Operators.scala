
package com.brkyvz.spark.linalg

object VectorOperators {

  def add(x: VectorLike, y: VectorLike): LazyVector = new LazyDenseVVOp(x, y, _ + _, None)
  def sub(x: VectorLike, y: VectorLike): LazyVector = new LazyDenseVVOp(x, y, _ - _, None)
  def mul(x: VectorLike, y: VectorLike): LazyVector = new LazyDenseVVOp(x, y, _ * _, None)
  def div(x: VectorLike, y: VectorLike): LazyVector = new LazyDenseVVOp(x, y, _ / _, None)
  def addInto(x: VectorLike, y: VectorLike, into: DenseVectorWrapper): LazyVector =
    new LazyDenseVVOp(x, y, _ + _, Option(into))
  def subInto(x: VectorLike, y: VectorLike, into: DenseVectorWrapper): LazyVector =
    new LazyDenseVVOp(x, y, _ - _, Option(into))
  def mulInto(x: VectorLike, y: VectorLike, into: DenseVectorWrapper): LazyVector =
    new LazyDenseVVOp(x, y, _ * _, Option(into))
  def divInto(x: VectorLike, y: VectorLike, into: DenseVectorWrapper): LazyVector =
    new LazyDenseVVOp(x, y, _ / _, Option(into))

  def log(x: VectorLike): LazyVector = new LazyDenseVOp(x, math.log, None)
}

object MatrixOperators {

  def add(x: MatrixLike, y: MatrixLike): LazyMatrix = new LazyImDenseMMOp(x, y, _ + _)
  def sub(x: MatrixLike, y: MatrixLike): LazyMatrix = new LazyImDenseMMOp(x, y, _ - _)
  def mul(x: MatrixLike, y: MatrixLike): LazyMatrix = new LazyImDenseMMOp(x, y, _ * _)
  def div(x: MatrixLike, y: MatrixLike): LazyMatrix = new LazyImDenseMMOp(x, y, _ / _)
  def log(x: MatrixLike): LazyMatrix = new LazyImDenseMOp(x, math.log)
}
