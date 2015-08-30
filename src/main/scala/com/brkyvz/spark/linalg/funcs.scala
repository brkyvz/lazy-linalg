package com.brkyvz.spark.linalg

class funcs {

  //////////////////////////////////////////////////
  // Matrix Functions
  //////////////////////////////////////////////////

  def sin(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.sin)
  def cos(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.cos)
  def tan(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.tan)
  def asin(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.asin)
  def acos(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.acos)
  def atan(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.atan)

  /** Converts an angle measured in degrees to an approximately equivalent
    *  angle measured in radians.
    *
    *  @param  x an angle, in degrees
    *  @return the measurement of the angle `x` in radians.
    */
  def toRadians(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.toRadians)

  /** Converts an angle measured in radians to an approximately equivalent
    *  angle measured in degrees.
    *
    *  @param  x angle, in radians
    *  @return the measurement of the angle `x` in degrees.
    */
  def toDegrees(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.toDegrees)

  /** Returns Euler's number `e` raised to the power of a `double` value.
    *
    *  @param  x the exponent to raise `e` to.
    *  @return the value `e^a^`, where `e` is the base of the natural
    *          logarithms.
    */
  def exp(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.exp)
  def log(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.log)
  def sqrt(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.sqrt)

  def ceil(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.ceil)
  def floor(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.floor)

  /** Returns the `double` value that is closest in value to the
    *  argument and is equal to a mathematical integer.
    *
    *  @param  x a `double` value
    *  @return the closest floating-point value to a that is equal to a
    *          mathematical integer.
    */
  def rint(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.rint)

  /** Converts rectangular coordinates `(x, y)` to polar `(r, theta)`.
    *
    *  @param  x the ordinate coordinate
    *  @param  y the abscissa coordinate
    *  @return the ''theta'' component of the point `(r, theta)` in polar
    *          coordinates that corresponds to the point `(x, y)` in
    *          Cartesian coordinates.
    */
  def atan2(y: MatrixLike, x: MatrixLike): MatrixLike = java.lang.Math.atan2(y, x)

  /** Returns the value of the first argument raised to the power of the
    *  second argument.
    *
    *  @param x the base.
    *  @param y the exponent.
    *  @return the value `x^y^`.
    */
  def pow(x: MatrixLike, y: MatrixLike): MatrixLike = java.lang.Math.pow(x, y)

  /** Returns the closest `long` to the argument.
    *
    *  @param  x a floating-point value to be rounded to a `long`.
    *  @return the value of the argument rounded to the nearest`long` value.
    */
  def round(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.round)

  def abs(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.abs)

  def max(x: MatrixLike, y: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.max(x, y)

  def min(x: MatrixLike, y: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.min(x, y)

  def signum(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.signum)

  // -----------------------------------------------------------------------
  // root functions
  // -----------------------------------------------------------------------

  /** Returns the cube root of the given `MatrixLike` value. */
  def cbrt(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.cbrt)

  // -----------------------------------------------------------------------
  // exponential functions
  // -----------------------------------------------------------------------

  /** Returns `exp(x) - 1`. */
  def expm1(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.expm1)

  // -----------------------------------------------------------------------
  // logarithmic functions
  // -----------------------------------------------------------------------

  /** Returns the natural logarithm of the sum of the given `MatrixLike` value and 1. */
  def log1p(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.log1p)

  /** Returns the base 10 logarithm of the given `MatrixLike` value. */
  def log10(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.log10)

  // -----------------------------------------------------------------------
  // trigonometric functions
  // -----------------------------------------------------------------------

  /** Returns the hyperbolic sine of the given `MatrixLike` value. */
  def sinh(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.sinh)

  /** Returns the hyperbolic cosine of the given `MatrixLike` value. */
  def cosh(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.cosh)

  /** Returns the hyperbolic tangent of the given `MatrixLike` value. */
  def tanh(x: MatrixLike): MatrixLike = new LazyImDenseMOp(x, java.lang.Math.tanh)

  // -----------------------------------------------------------------------
  // miscellaneous functions
  // -----------------------------------------------------------------------

  /** Returns the square root of the sum of the squares of both given `MatrixLike`
    * values without intermediate underflow or overflow.
    */
  def hypot(x: MatrixLike, y: MatrixLike): MatrixLike = java.lang.Math.hypot(x, y)


  //////////////////////////////////////////////////
  // Vector Functions
  //////////////////////////////////////////////////

  def sin(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.sin)
  def cos(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.cos)
  def tan(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.tan)
  def asin(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.asin)
  def acos(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.acos)
  def atan(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.atan)

  /** Converts an angle measured in degrees to an approximately equivalent
    *  angle measured in radians.
    *
    *  @param  x an angle, in degrees
    *  @return the measurement of the angle `x` in radians.
    */
  def toRadians(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.toRadians)

  /** Converts an angle measured in radians to an approximately equivalent
    *  angle measured in degrees.
    *
    *  @param  x angle, in radians
    *  @return the measurement of the angle `x` in degrees.
    */
  def toDegrees(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.toDegrees)

  /** Returns Euler's number `e` raised to the power of a `double` value.
    *
    *  @param  x the exponent to raise `e` to.
    *  @return the value `e^a^`, where `e` is the base of the natural
    *          logarithms.
    */
  def exp(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.exp)
  def log(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.log)
  def sqrt(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.sqrt)

  def ceil(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.ceil)
  def floor(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.floor)

  /** Returns the `double` value that is closest in value to the
    *  argument and is equal to a mathematical integer.
    *
    *  @param  x a `double` value
    *  @return the closest floating-point value to a that is equal to a
    *          mathematical integer.
    */
  def rint(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.rint)

  /** Converts rectangular coordinates `(x, y)` to polar `(r, theta)`.
    *
    *  @param  x the ordinate coordinate
    *  @param  y the abscissa coordinate
    *  @return the ''theta'' component of the point `(r, theta)` in polar
    *          coordinates that corresponds to the point `(x, y)` in
    *          Cartesian coordinates.
    */
  def atan2(y: VectorLike, x: VectorLike): VectorLike = java.lang.Math.atan2(y, x)

  /** Returns the value of the first argument raised to the power of the
    *  second argument.
    *
    *  @param x the base.
    *  @param y the exponent.
    *  @return the value `x^y^`.
    */
  def pow(x: VectorLike, y: VectorLike): VectorLike = java.lang.Math.pow(x, y)

  /** Returns the closest `long` to the argument.
    *
    *  @param  x a floating-point value to be rounded to a `long`.
    *  @return the value of the argument rounded to the nearest`long` value.
    */
  def round(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.round)
  def abs(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.abs)

  def max(x: VectorLike, y: VectorLike): VectorLike = java.lang.Math.max(x, y)

  def min(x: VectorLike, y: VectorLike): VectorLike = java.lang.Math.min(x, y)

  def signum(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.signum)

  // -----------------------------------------------------------------------
  // root functions
  // -----------------------------------------------------------------------

  /** Returns the cube root of the given `VectorLike` value. */
  def cbrt(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.cbrt)

  // -----------------------------------------------------------------------
  // exponential functions
  // -----------------------------------------------------------------------

  /** Returns `exp(x) - 1`. */
  def expm1(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.expm1)

  // -----------------------------------------------------------------------
  // logarithmic functions
  // -----------------------------------------------------------------------

  /** Returns the natural logarithm of the sum of the given `VectorLike` value and 1. */
  def log1p(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.log1p)

  /** Returns the base 10 logarithm of the given `VectorLike` value. */
  def log10(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.log10)

  // -----------------------------------------------------------------------
  // trigonometric functions
  // -----------------------------------------------------------------------

  /** Returns the hyperbolic sine of the given `VectorLike` value. */
  def sinh(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.sinh)

  /** Returns the hyperbolic cosine of the given `VectorLike` value. */
  def cosh(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.cosh)

  /** Returns the hyperbolic tangent of the given `VectorLike` value. */
  def tanh(x: VectorLike): VectorLike = new LazyDenseVOp(x, java.lang.Math.tanh)

  // -----------------------------------------------------------------------
  // miscellaneous functions
  // -----------------------------------------------------------------------

  /** Returns the square root of the sum of the squares of both given `VectorLike`
    * values without intermediate underflow or overflow.
    */
  def hypot(x: VectorLike, y: VectorLike): VectorLike = java.lang.Math.hypot(x, y)

}
