% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/operations.R
\name{rowSums,dbDenseMatrix-method}
\alias{rowSums,dbDenseMatrix-method}
\alias{rowSums,dbSparseMatrix-method}
\alias{colSums,dbDenseMatrix-method}
\alias{colSums,dbSparseMatrix-method}
\title{Row (column) sums for dbMatrix objects}
\usage{
\S4method{rowSums}{dbDenseMatrix}(x, ..., memory = FALSE)

\S4method{rowSums}{dbSparseMatrix}(x, ..., memory = FALSE)

\S4method{colSums}{dbDenseMatrix}(x, ..., memory = FALSE)

\S4method{colSums}{dbSparseMatrix}(x, ..., memory = FALSE)
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
Calculates the sum for each row (column) of a matrix-like object.
}
\concept{summary}
