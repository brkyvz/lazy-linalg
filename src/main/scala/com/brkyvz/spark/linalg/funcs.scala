package com.brkyvz.spark.linalg

object funcs {

  //////////////////////////////////////////////////
  // Matrix Functions
  //////////////////////////////////////////////////

  def add(x: MatrixLike, y: MatrixLike): LazyMatrix = new LazyImDenseMMOp(x, y, _ + _)
  def sub(x: MatrixLike, y: MatrixLike): LazyMatrix = new LazyImDenseMMOp(x, y, _ - _)
  def emul(x: MatrixLike, y: MatrixLike): LazyMatrix = new LazyImDenseMMOp(x, y, _ * _)
  def div(x: MatrixLike, y: MatrixLike): LazyMatrix = new LazyImDenseMMOp(x, y, _ / _)

  def apply(x: MatrixLike, y: MatrixLike, f: (Double, Double) => Double): LazyMatrix =
    new LazyImDenseMMOp(x, y, f)

  def apply(x: MatrixLike, f: (Double) => Double): LazyMatrix = new LazyMatrixMapOp(x, f)

  def sin(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.sin)
  def cos(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.cos)
  def tan(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.tan)
  def asin(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.asin)
  def acos(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.acos)
  def atan(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.atan)

  /** Converts an angle measured in degrees to an approximately equivalent
    *  angle measured in radians.
    *
    *  @param  x an angle, in degrees
    *  @return the measurement of the angle `x` in radians.
    */
  def toRadians(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.toRadians)

  /** Converts an angle measured in radians to an approximately equivalent
    *  angle measured in degrees.
    *
    *  @param  x angle, in radians
    *  @return the measurement of the angle `x` in degrees.
    */
  def toDegrees(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.toDegrees)

  /** Returns Euler's number `e` raised to the power of a `double` value.
    *
    *  @param  x the exponent to raise `e` to.
    *  @return the value `e^a^`, where `e` is the base of the natural
    *          logarithms.
    */
  def exp(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.exp)
  def log(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.log)
  def sqrt(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.sqrt)

  def ceil(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.ceil)
  def floor(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.floor)

  /** Returns the `double` value that is closest in value to the
    *  argument and is equal to a mathematical integer.
    *
    *  @param  x a `double` value
    *  @return the closest floating-point value to a that is equal to a
    *          mathematical integer.
    */
  def rint(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.rint)

  /** Converts rectangular coordinates `(x, y)` to polar `(r, theta)`.
    *
    *  @param  x the ordinate coordinate
    *  @param  y the abscissa coordinate
    *  @return the ''theta'' component of the point `(r, theta)` in polar
    *          coordinates that corresponds to the point `(x, y)` in
    *          Cartesian coordinates.
    */
  def atan2(y: MatrixLike, x: MatrixLike): MatrixLike =
    new LazyImDenseMMOp(y, x, java.lang.Math.atan2)

  /** Returns the value of the first argument raised to the power of the
    *  second argument.
    *
    *  @param x the base.
    *  @param y the exponent.
    *  @return the value `x^y^`.
    */
  def pow(x: MatrixLike, y: MatrixLike): LazyMatrix = new LazyImDenseMMOp(x, y, java.lang.Math.pow)

  def abs(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.abs)

  def max(x: MatrixLike, y: MatrixLike): LazyMatrix = new LazyImDenseMMOp(x, y, java.lang.Math.max)

  def min(x: MatrixLike, y: MatrixLike): LazyMatrix = new LazyImDenseMMOp(x, y, java.lang.Math.min)

  def signum(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.signum)

  // -----------------------------------------------------------------------
  // root functions
  // -----------------------------------------------------------------------

  /** Returns the cube root of the given `MatrixLike` value. */
  def cbrt(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.cbrt)

  // -----------------------------------------------------------------------
  // exponential functions
  // -----------------------------------------------------------------------

  /** Returns `exp(x) - 1`. */
  def expm1(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.expm1)

  // -----------------------------------------------------------------------
  // logarithmic functions
  // -----------------------------------------------------------------------

  /** Returns the natural logarithm of the sum of the given `MatrixLike` value and 1. */
  def log1p(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.log1p)

  /** Returns the base 10 logarithm of the given `MatrixLike` value. */
  def log10(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.log10)

  // -----------------------------------------------------------------------
  // trigonometric functions
  // -----------------------------------------------------------------------

  /** Returns the hyperbolic sine of the given `MatrixLike` value. */
  def sinh(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.sinh)

  /** Returns the hyperbolic cosine of the given `MatrixLike` value. */
  def cosh(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.cosh)

  /** Returns the hyperbolic tangent of the given `MatrixLike` value. */
  def tanh(x: MatrixLike): LazyMatrix = new LazyMatrixMapOp(x, java.lang.Math.tanh)

  // -----------------------------------------------------------------------
  // miscellaneous functions
  // -----------------------------------------------------------------------

  /** Returns the square root of the sum of the squares of both given `MatrixLike`
    * values without intermediate underflow or overflow.
    */
  def hypot(x: MatrixLike, y: MatrixLike): LazyMatrix =
    new LazyImDenseMMOp(x, y, java.lang.Math.hypot)


  //////////////////////////////////////////////////
  // Vector Functions
  //////////////////////////////////////////////////

  def add(x: VectorLike, y: VectorLike): LazyVector = {
    (x, y) match {
      case (a: SparseVectorWrapper, b: SparseVectorWrapper) => new LazySparseVVOp(a, b, _ + _)
      case _ => new LazyDenseVVOp(x, y, _ + _)
    }
  }
  def sub(x: VectorLike, y: VectorLike): LazyVector = {
    (x, y) match {
      case (a: SparseVectorWrapper, b: SparseVectorWrapper) => new LazySparseVVOp(a, b, _ - _)
      case _ => new LazyDenseVVOp(x, y, _ - _)
    }
  }
  def emul(x: VectorLike, y: VectorLike): LazyVector = {
    (x, y) match {
      case (a: SparseVectorWrapper, b: SparseVectorWrapper) => new LazySparseVVOp(a, b, _ * _)
      case _ => new LazyDenseVVOp(x, y, _ * _)
    }
  }
  def div(x: VectorLike, y: VectorLike): LazyVector = {
    (x, y) match {
      case (a: SparseVectorWrapper, b: SparseVectorWrapper) => new LazySparseVVOp(a, b, _ / _)
      case _ => new LazyDenseVVOp(x, y, _ / _)
    }
  }

  def apply(x: VectorLike, y: VectorLike, f: (Double, Double) => Double): LazyVector =
    new LazyDenseVVOp(x, y, f)

  def apply(x: VectorLike, y: Scalar, f: (Double, Double) => Double): LazyVector =
    new LazyDenseVSOp(x, y, f)

  def apply(x: Scalar, y: VectorLike, f: (Double, Double) => Double): LazyVector =
    new LazyDenseSVOp(x, y, f)

  def apply(x: VectorLike, f: (Double) => Double): LazyVector = new LazyVectorMapOp(x, f)

  def sin(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.sin)
  def cos(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.cos)
  def tan(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.tan)
  def asin(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.asin)
  def acos(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.acos)
  def atan(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.atan)

  /** Converts an angle measured in degrees to an approximately equivalent
    *  angle measured in radians.
    *
    *  @param  x an angle, in degrees
    *  @return the measurement of the angle `x` in radians.
    */
  def toRadians(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.toRadians)

  /** Converts an angle measured in radians to an approximately equivalent
    *  angle measured in degrees.
    *
    *  @param  x angle, in radians
    *  @return the measurement of the angle `x` in degrees.
    */
  def toDegrees(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.toDegrees)

  /** Returns Euler's number `e` raised to the power of a `double` value.
    *
    *  @param  x the exponent to raise `e` to.
    *  @return the value `e^a^`, where `e` is the base of the natural
    *          logarithms.
    */
  def exp(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.exp)
  def log(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.log)
  def sqrt(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.sqrt)

  def ceil(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.ceil)
  def floor(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.floor)

  /** Returns the `double` value that is closest in value to the
    *  argument and is equal to a mathematical integer.
    *
    *  @param  x a `double` value
    *  @return the closest floating-point value to a that is equal to a
    *          mathematical integer.
    */
  def rint(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.rint)

  /** Converts rectangular coordinates `(x, y)` to polar `(r, theta)`.
    *
    *  @param  x the ordinate coordinate
    *  @param  y the abscissa coordinate
    *  @return the ''theta'' component of the point `(r, theta)` in polar
    *          coordinates that corresponds to the point `(x, y)` in
    *          Cartesian coordinates.
    */
  def atan2(y: VectorLike, x: VectorLike): LazyVector =
    new LazyDenseVVOp(y, x, java.lang.Math.atan2)

  /** Returns the value of the first argument raised to the power of the
    *  second argument.
    *
    *  @param x the base.
    *  @param y the exponent.
    *  @return the value `x^y^`.
    */
  def pow(x: VectorLike, y: VectorLike): LazyVector = new LazyDenseVVOp(x, y, java.lang.Math.pow)

  def abs(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.abs)

  def max(x: VectorLike, y: VectorLike): LazyVector = new LazyDenseVVOp(x, y, java.lang.Math.max)

  def min(x: VectorLike, y: VectorLike): LazyVector = new LazyDenseVVOp(x, y, java.lang.Math.min)

  def signum(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.signum)

  // -----------------------------------------------------------------------
  // root functions
  // -----------------------------------------------------------------------

  /** Returns the cube root of the given `VectorLike` value. */
  def cbrt(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.cbrt)

  // -----------------------------------------------------------------------
  // exponential functions
  // -----------------------------------------------------------------------

  /** Returns `exp(x) - 1`. */
  def expm1(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.expm1)

  // -----------------------------------------------------------------------
  // logarithmic functions
  // -----------------------------------------------------------------------

  /** Returns the natural logarithm of the sum of the given `VectorLike` value and 1. */
  def log1p(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.log1p)

  /** Returns the base 10 logarithm of the given `VectorLike` value. */
  def log10(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.log10)

  // -----------------------------------------------------------------------
  // trigonometric functions
  // -----------------------------------------------------------------------

  /** Returns the hyperbolic sine of the given `VectorLike` value. */
  def sinh(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.sinh)

  /** Returns the hyperbolic cosine of the given `VectorLike` value. */
  def cosh(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.cosh)

  /** Returns the hyperbolic tangent of the given `VectorLike` value. */
  def tanh(x: VectorLike): LazyVector = new LazyVectorMapOp(x, java.lang.Math.tanh)

  // -----------------------------------------------------------------------
  // miscellaneous functions
  // -----------------------------------------------------------------------

  /** Returns the square root of the sum of the squares of both given `VectorLike`
    * values without intermediate underflow or overflow.
    */
  def hypot(x: VectorLike, y: VectorLike): LazyVector =
    new LazyDenseVVOp(x, y, java.lang.Math.hypot)

}
