<!-- NEWS.md is maintained by https://cynkra.github.io/fledge, do not edit -->

# dbMatrix 0.0.0.9023 (2024-09-18)

## Breaking Changes 

- dbMatrix summary methods now return `dbDenseMatrix` objects instead of in-memory vectors.

- Removed colTypes from castNumeric.

- Removed @name check in initialize.

## Features

- New `compute` method for saving dbMatrix objects. This writes the dbMatrix object to a table in the database along with its row and column names (e.g. 'dbMatrixname_rownames').

- New `load` method for loading computed dbMatrix objects. The dbMatrix::compute() method must be called before loading the object.

- Updated `.check_overwrite` to allow for passing overwrite arg.

- New internal function `write_dimnames` to enable saving dbMatrix objects.

- New `sum` method for dbMatrix objects.

- Improvements to precomputed table: Use existing precomputed table in db if available.

- Improvements to precomputed table: Transpose precomputed table of sufficient dimensions if it exists instead of creating a new one. This is done via a TEMPORARY VIEW to avoid writing to disk and modifying existing precomputed table which may be referenced by other tables.

- Custom SQL statement for `todbDense` conversion creates a TEMPORARY VIEW with name `dbDenseMatrix_hash`. 

## Chore 

- Update whitespace.

- Update dbMatrix constructor roxygen.

- Update roxygen in toDbDense.

## Docs

- Update reference page.

- Update mean documentation.

- Update log documentation.

- Update roxygen.

## Bug fixes

- Do not recycle matrix if not needed.

# dbMatrix 0.0.0.9022 (2024-08-14)

## Breaking changes

- Rename dbMatrix constructor to `dbMatrix::dbMatrix()`.

## Features

- Add more tests for `dbMatrix` and `dbDenseMatrix`.

- Add internal function `map_ijx_dimnames`.

- Add internal function `dbMatrix_from_tbl`.

- Extract now constructs unique temporary virtual tables in the arrow schema.

## Chore

- Document pkgdown website. 

- Add `cli` to imports.

- Update generics.

## Bug fixes

- Be more explicit about non-supported `Arith` and `Ops` operations.

- Fix `show()` method for `dbDenseMatrix` objects.


# dbMatrix 0.0.0.9021 (2024-07-05)

## Bug fixes

- Improve show function for dbDenseMatrix.

- Updated simulate functions to include overwrite param.

- Update .check_overwrite() internal function to avoid bug in overwritting passed table.


# dbMatrix 0.0.0.9020 (2024-07-05)

## Features

- Add initial support for reading in .mtx files and creating dbMatrix objects.

## Bug fixes

- Update `check_overwrite` input validation

## Chore

- Update Royxgen


# dbMatrix 0.0.0.9019 (2024-03-18)

##  Features

- Add new `precompute` function to speed up matrix densification.

- Add new show function for dbDenseMatrix with pretty color and better spacing.

- Add new `save` function to save a `dbMatrix`. 

- Add new input validation functions.


## Chore

- Update docs. 

- Update imports to include `glue`, `bit64` and `crayon`. 

<!-- NEWS.md is maintained by https://fledge.cynkra.com, contributors should not edit this file -->

# dbMatrix 0.0.0.9018 (2024-02-12)

## Bug fixes

- Update constructor calls in `sim` functions.

- Remove redundant `con` from constructor.

- Remove `db_path` from constructor.

- Constructor `db_path` arg change to `con` object.

## Features

- Add `dgTMatrix` to in-memory matrix types supported in `dbMatrix` constructor.

## Chore

- Set :memory: to default db_path in constructor.

- Update createDBMatrix docs.

## Documentation

- Spacing.

- Update after constructor fix.

- Remove :temp: in place of :memory:.


# dbMatrix 0.0.0.9017 (2024-02-07)

## Bug fixes

- Remove ":temp:" from tests.

- Replace ':temp:' with ':memory:'.

- Add matrix in addition to dgCMatrix in as_ijx().

## Features

- Add unit tests for scalar arith.

- Add unit tests for names.R.

## Chore

- Remove random browser() call.

- Add `MatrixGenerics` to deps.

- Update gitignore.


# dbMatrix 0.0.0.9016 (2024-01-23)

## Features

- Add boolean indexing tests to `test-extract.R`.

- Add as_ijx() convenience function.

- Update createDBMatrix() to use dplyr::copy_to().

- Update as_matrix() convenience function.

- Add unit tests for extract methods.


# dbMatrix 0.0.0.9015 (2024-01-22)

## Features

- Add log().

- Update toDbDense to use dplyr instead of SQL.

## Chore

- Update DESCRIPTION to include testthat.

- Update docs.


# dbMatrix 0.0.0.9014 (2024-01-19)

## Features

- update dbIndex superclass

- update as_matrix() 

# dbMatrix 0.0.0.9013 (2024-01-18)

## Bug fixes

- Rename and update dbIndex superclass to fix indexing bugs.

## Features

- Add experimental as_matrix() convenience function.

## Chore

- Update docs.

- Migrate site link to drieslab.

- Move {Matrix} to Imports.

## Documentation

- Update overview.Rmd.

- Update operations vignette.


# dbMatrix 0.0.0.9012 (2023-12-08)

## Bug fixes

- Only densify if necessary. != 0, +/-.

- toDbDense() previously updated table by value. change to update by reference via VIEW creation of table named 'dense'.

## Features

- Add dbListTables().

## Chore

- Update docs.

- Update gitignore.


# dbMatrix 0.0.0.9011 (2023-12-08)

## Bug fixes

- Fix incorrect aggregate operations.

- Fixes after con slot removal.

## Features

- Add get_con().

- Add dbDisconnect() generic.

- Update accessors.

## Chore

- Update vignettes.

- Update README.

- Update gitignore.

- Update docs.

- Update NEWS.


# dbMatrix 0.0.0.9010 (2023-11-25)

## Breaking changes

- migrate to |> pipe, remove %>%, update deps.

## Chore

- Update docs.


<!-- NEWS.md is maintained by https://fledge.cynkra.com, contributors should not edit this file -->

# dbMatrix 0.0.0.9009 (2023-11-20)

## Bug fixes

- Mean generic for dbDenseMatrix.

## Chore

- Update docs.

- Update site.

- Update gitignore.


# dbMatrix 0.0.0.9008 (2023-11-17)

## Bug fixes

- Updates to dbSparseMatrix rowMeans, colMeans.

- Updates to colSums and rowSums for dbSparseMatrix.

- Update dbMatrix constructor dimnames issues.

## Chore

- Update DESCRIPTION, remove redundant Matrix import, dep.

- Update docs.

- Update roxygen for operations.


# dbMatrix 0.0.0.9007 (2023-11-09)

## Features

- Add Matrix as dep.


# dbMatrix 0.0.0.9006 (2023-11-09)

## Bug fixes

- Methods::as scope correction.

## Features

- Add methods to deps.

## Chore

- Update .gitignore.

# dbMatrix 0.0.0.9005 (2023-11-09)

## Feat

- Add Matrix as pkg dep


# dbMatrix 0.0.0.9004 (2023-11-09)

## Bug fixes

- Missing Matrix:: scope call.

## Chore

- Update docs.

# dbMatrix 0.0.0.9003 (2023-11-07)

## Fix

- `dims` and `dim_names` in `createDBMatrix()` retained from in-memory matrix or Matrix object

## Chore

- Update docs for dbMatrix input checks

# dbMatrix 0.0.0.9002 (2023-11-07)

## Feat
- Specify checks for 'name' param in `createDBMatrix()`

## Chore

- Update docs.

- Add docs.


# dbMatrix 0.0.0.9001 (2023-11-04)

## Chore

- Update docs and DESCRIPTION.

- Migration dbMatrix.
