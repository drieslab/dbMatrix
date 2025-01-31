% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/operations.R
\name{rowMeans,dbDenseMatrix-method}
\alias{rowMeans,dbDenseMatrix-method}
\alias{rowMeans,dbSparseMatrix-method}
\alias{colMeans,dbDenseMatrix-method}
\alias{colMeans,dbSparseMatrix-method}
\title{Row (column) means for dbMatrix objects}
\usage{
\S4method{rowMeans}{dbDenseMatrix}(x, ..., memory = FALSE)

\S4method{rowMeans}{dbSparseMatrix}(x, ..., memory = FALSE)

\S4method{colMeans}{dbDenseMatrix}(x, ..., memory = FALSE)

\S4method{colMeans}{dbSparseMatrix}(x, ..., memory = FALSE)
}
\arguments{
\item{x}{An NxK matrix-like object, a numeric data frame, or an array-like
object of two or more dimensions.}

\item{...}{Additional arguments passed to specific methods.}

\item{memory}{logical. If FALSE (default), results returned as dbDenseMatrix. This is recommended
for large computations. Set to TRUE to return the results as a vector.}

\item{na.rm}{Always TRUE for dbMatrix queries. Included for compatibility
with the generic.}

\item{dims}{Always 1 for dbMatrix queries. Included for compatibility with
the generic.}
}
\description{
Calculates the mean for each row (column) of a matrix-like object.
}
\concept{summary}
