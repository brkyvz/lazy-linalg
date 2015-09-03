lazy-linalg
-----------

A package full of linear algebra operators for Apache Spark MLlib's linalg package.
Works best with Spark 1.5.

Goal
====

Most of the code in this repository was written as a part of
[SPARK-6442](https://issues.apache.org/jira/browse/SPARK-6442). The aim was to support the most
common local linear algebra operations on top of Spark without having to depend on an external
library.

It is somewhat cumbersome to write code where you have to convert the MLlib representation of a
vector or matrix to Breeze perform the simplest arithmetic operations like addition, subtraction, etc.
This package aims to lift that burden, and provide efficient implementations for some of these methods.

By keeping operations lazy, this package provides some of the optimizations that you would see
in C++ libraries like Armadillo, Eigen, etc.

Installation
============



Examples
========

Import `com.brkyvz.spark.linalg._` and all the implicits will kick in for Scala users.

```scala
scala> import com.brkyvz.spark.linalg._
scala> import org.apache.spark.mllib.linalg._

scala> val rnd = new java.util.Random

scala> val A = DenseMatrix.eye(3)
A: org.apache.spark.mllib.linalg.DenseMatrix =
1.0  0.0  0.0
0.0  1.0  0.0
0.0  0.0  1.0

scala> val B = DenseMatrix.rand(3, 3, rnd)
B: org.apache.spark.mllib.linalg.DenseMatrix =
0.6133402813080373   0.7162729054788076   0.15011768207263143
0.3078993912354502   0.23923486751376188  0.05973497171994935
0.49892408305838276  0.9534484503645188   0.48047741591983717

scala> val C = DenseMatrix.zeros(3, 3)
C: org.apache.spark.mllib.linalg.DenseMatrix =
0.0  0.0  0.0
0.0  0.0  0.0
0.0  0.0  0.0

scala> C := A + B + A * B
res0: com.brkyvz.spark.linalg.MatrixLike =
2.2266805626160746  1.4325458109576152  0.30023536414526286
0.6157987824709004  1.4784697350275238  0.1194699434398987
0.9978481661167655  1.9068969007290375  1.9609548318396746

scala> C += -1
res1: com.brkyvz.spark.linalg.DenseMatrixWrapper =
1.2266805626160746      0.43254581095761524  -0.6997646358547371
-0.38420121752909964    0.47846973502752377  -0.8805300565601013
-0.0021518338832344774  0.9068969007290375   0.9609548318396746

scala> val D = A * 2 - 1
scala> D.compute()
res2: com.brkyvz.spark.linalg.MatrixLike =
1.0   -1.0  -1.0
-1.0  1.0   -1.0
-1.0  -1.0  1.0
```

Matrix multiplication vs. element-wise multiplication:

```scala
scala> C := A * B
res4: com.brkyvz.spark.linalg.MatrixLike =
0.6133402813080373   0.7162729054788076   0.15011768207263143
0.3078993912354502   0.23923486751376188  0.05973497171994935
0.49892408305838276  0.9534484503645188   0.48047741591983717

scala> C := A :* B
res5: com.brkyvz.spark.linalg.MatrixLike =
0.6133402813080373  0.0                  0.0
0.0                 0.23923486751376188  0.0
0.0                 0.0                  0.48047741591983717
```

Support for element-wise basic math functions:

```scala
import com.brkyvz.spark.linalg.funcs._

scala> C := pow(B, A)
res7: com.brkyvz.spark.linalg.MatrixLike =
0.6133402813080373  1.0                  1.0
1.0                 0.23923486751376188  1.0
1.0                 1.0                  0.48047741591983717

scala> C := exp(A)
res8: com.brkyvz.spark.linalg.MatrixLike =
2.718281828459045  1.0                1.0
1.0                2.718281828459045  1.0
1.0                1.0                2.718281828459045

scala> C := asin(A) - math.Pi / 2
res12: com.brkyvz.spark.linalg.MatrixLike =
0.0                  -1.5707963267948966  -1.5707963267948966
-1.5707963267948966  0.0                  -1.5707963267948966
-1.5707963267948966  -1.5707963267948966  0.0
```

All operations work similarly for vectors.

```scala
scala> import com.brkyvz.spark.linalg._
scala> import org.apache.spark.mllib.linalg._

scala> val x = Vectors.dense(1, 0, 2, 1)
x: org.apache.spark.mllib.linalg.Vector = [1.0,0.0,2.0,1.0]

scala> val y = Vectors.dense(0, 0, 0, 0)
y: org.apache.spark.mllib.linalg.Vector = [0.0,0.0,0.0,0.0]

scala> y := x / 2 + x * 0.5
res15: com.brkyvz.spark.linalg.DenseVectorWrapper = [1.0,0.0,2.0,1.0]

scala> val z = Vectors.dense(1, -2, 3)
z: org.apache.spark.mllib.linalg.Vector = [1.0,-2.0,3.0]

scala> val m = A * z + 1
scala> m.compute()
res16: com.brkyvz.spark.linalg.VectorLike = [2.0,-1.0,4.0]
```

Caveats
=======

1- Implicits may not work perfectly for vectors and matrices generated through `Matrices.` and
`Vectors.` consutructors.

2- Scalars need to be **after** the matrix or the vector during operations, e.g. use `x * 2` instead
 of `2 * x`.

What's to Come
==============

1- Support for more linear algebra operations: determinant, matrix inverse, trace, slicing, 
reshaping...

2- Supporting such methods with BlockMatrix.

3- Better, smarter lazy evaluation with codegen. This should bring performance closer to C++
 libraries.


Contributing
============

If you run across any bugs, please file issues, and please feel free to submit pull requests!


