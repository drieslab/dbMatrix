# Reference 

## As.spatvector

### Description

Convert to terra spatvector

### Usage

    ## S4 method for signature 'dbSpatProxyData'
    as.spatvector(x, ...)


---
## BackendID-generic

### Description

Get the hash value/slot

### Usage

    ## S4 method for signature 'backendInfo'
    backendID(x)

    ## S4 method for signature 'dbData'
    backendID(x)

    ## S4 replacement method for signature 'backendInfo,character'
    backendID(x, ...) <- value

    ## S4 replacement method for signature 'dbData,character'
    backendID(x, ...) <- value

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="backendID-generic_:_x">x</code></td>
<td><p>object to get hash from</p></td>
</tr>
<tr class="even">
<td><code id="backendID-generic_:_value">value</code></td>
<td><p>(character) hash ID to set</p></td>
</tr>
</tbody>
</table>


---
## BackendInfo

### Description

Simple S4 class to contain information about the database backend and
regenerate connection pools. A hash is generated from the db\_path to
act as a unique identifier for each backend.

### Slots

`driver_call`  
DB driver call stored as a string

`db_path`  
path to database

`hash`  
xxhash64 hash of the db\_path


---
## BackendSize

### Description

Given a backend ID, find the current size of the DB backend file

### Usage

    backendSize(backend_ID)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="backendSize_:_backend_ID">backend_ID</code></td>
<td><p>backend ID</p></td>
</tr>
</tbody>
</table>


---
## Callback combineCols

### Description

Combine columns values

### Usage

    callback_combineCols(x, col_indices, new_col = "new", remove_originals = TRUE)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="callback_combineCols_:_x">x</code></td>
<td><p>data.table</p></td>
</tr>
<tr class="even">
<td><code
id="callback_combineCols_:_col_indices">col_indices</code></td>
<td><p>numeric vector. Col indices to combine</p></td>
</tr>
<tr class="odd">
<td><code id="callback_combineCols_:_new_col">new_col</code></td>
<td><p>name of new combined column</p></td>
</tr>
<tr class="even">
<td><code
id="callback_combineCols_:_remove_originals">remove_originals</code></td>
<td><p>remove originals cols of the combined col default = TRUE</p></td>
</tr>
</tbody>
</table>

### Value

data.table with specified column combined


---
## Callback formatIJX

### Description

Building block function intended for use as or in callback functions
used when streaming large flat files to database. Converts the input
data.table to long format with columns i, j, and x where i and j are row
and col names and x is values. Columns i and j are additionally set as
'character'.

### Usage

    callback_formatIJX(x, group_by = 1)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="callback_formatIJX_:_x">x</code></td>
<td><p>data.table</p></td>
</tr>
<tr class="even">
<td><code id="callback_formatIJX_:_group_by">group_by</code></td>
<td><p>numeric or character designating which column of current data to
set as i. Default = 1st column</p></td>
</tr>
</tbody>
</table>

### Value

data.table in ijx format


---
## Callback swapCols

### Description

Swap values in two columns

### Usage

    callback_swapCols(x, c1, c2)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="callback_swapCols_:_x">x</code></td>
<td><p>data.table</p></td>
</tr>
<tr class="even">
<td><code id="callback_swapCols_:_c1">c1</code></td>
<td><p>col 1 to use (character)</p></td>
</tr>
<tr class="odd">
<td><code id="callback_swapCols_:_c2">c2</code></td>
<td><p>col 2 to use (character)</p></td>
</tr>
</tbody>
</table>

### Value

data.table with designated column values swapped


---
## CastNumeric

### Description

Sets a column to numeric after first checking the column data type. Does
nothing if the column is already a `double` This precaution is to avoid
truncation of values.

### Usage

    ## S4 method for signature 'dbData,character'
    castNumeric(x, col, ...)

    ## S4 method for signature 'dbMatrix,missing'
    castNumeric(x, col, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="castNumeric_:_x">x</code></td>
<td><p>Duckling data object</p></td>
</tr>
<tr class="even">
<td><code id="castNumeric_:_col">col</code></td>
<td><p>column to cast to numeric</p></td>
</tr>
<tr class="odd">
<td><code id="castNumeric_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>


---
## Chunk plan

### Description

Generate the individual extents that will be used to spatially chunk a
set of data for piecewise and potentially parallelized processing.
Chunks will be generated first by row, then by column. The chunks try to
be as square as possible since downstream functions may require slight
expansions of the extents to capture all parts of selected polygons.
Minimizing the perimeter relative to area decreases waste.

### Usage

    chunk_plan(extent, min_chunks = NULL, nrows = NULL, ncols = NULL)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="chunk_plan_:_extent">extent</code></td>
<td><p>terra SpatExtent that covers the region to spatially
chunk</p></td>
</tr>
<tr class="even">
<td><code id="chunk_plan_:_min_chunks">min_chunks</code></td>
<td><p>numeric. minimum number of chunks to use.</p></td>
</tr>
<tr class="odd">
<td><code id="chunk_plan_:_nrows">nrows</code>, <code
id="chunk_plan_:_ncols">ncols</code></td>
<td><p>numeric. nrow/ncol must be provided as a pair. Determines how
many rows and cols respectively will be used in spatial chunking. If
NULL, min_chunks will be used as an automated method of planning the
spatial chunking</p></td>
</tr>
</tbody>
</table>

### Value

vector of chunked SpatExtents

### See Also

`get_dim_n_chunks`

### Examples

```r
## Not run: 
 e <- terra::ext(0, 100, 0, 100)

e_chunk1 <- chunk_plan(e, min_chunks = 9)
e_poly1 <- sapply(e_chunk1, terra::as.polygons)
e_poly1 <- do.call(rbind, e_poly1)
plot(e_poly1)

e_chunk2 <- chunk_plan(e, nrows = 3, ncols = 5)
e_poly2 <- sapply(e_chunk2, terra::as.polygons)
e_poly2 <- do.call(rbind, e_poly2)
plot(e_poly2)

## End(Not run)
```


---
## Chunk spat apply

### Description

chunk\_plan slightly expands bounds, allowing for use of 'soft'
selections with 'extent\_filter() on two sides during the chunk
processing Calculations with y are expected to be performed relative to
x. Spatial chunk subsetting of y is performed based on the updated
extent of the x chunks after their chunk subset. 1. Setup lapply 2. Run
provided function 3. write or return values

### Usage

    chunk_spat_apply(
      x = NULL,
      y = NULL,
      chunk_y = TRUE,
      fun,
      extent = NULL,
      n_per_chunk = 1e+05,
      remote_name = NULL,
      output = c("tbl", "dbPolygonProxy", "dbPointsProxy"),
      progress = TRUE,
      ...
    )


---
## ChunkSpatApply

### Description

Split a function operation into multiple spatial chunks. Results are
appended back into the database as a new table. This is not parallelized
as some databases do not work with parallel writes, but are more
performant with large single chunks of data. The functions that are
provided, however, can be parallelized in their processing after the
chunk has been pulled into memory and only needs to be combined into one
before being written.

### Usage

    chunkSpatApplyPoly(
      x = NULL,
      y = NULL,
      chunk_y = TRUE,
      fun,
      extent = NULL,
      n_per_chunk = getOption("gdb.nperchunk", 1e+05),
      remote_name = NULL,
      progress = TRUE,
      ...
    )

    chunkSpatApplyPoints(
      x = NULL,
      y = NULL,
      chunk_y = TRUE,
      fun,
      extent = NULL,
      n_per_chunk = getOption("gdb.nperchunk", 1e+05),
      remote_name = NULL,
      progress = TRUE
    )

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="chunkSpatApply_:_x">x</code></td>
<td><p>dbPolygonProxy or dbPointsProxy</p></td>
</tr>
<tr class="even">
<td><code id="chunkSpatApply_:_y">y</code></td>
<td><p>missing/null if not needed. Otherwise accepts a dbPolygonProxy or
dbPointsProxy object</p></td>
</tr>
<tr class="odd">
<td><code id="chunkSpatApply_:_chunk_y">chunk_y</code></td>
<td><p>(default = TRUE) whether y also needs to be spatially chunked if
it is provided.</p></td>
</tr>
<tr class="even">
<td><code id="chunkSpatApply_:_fun">fun</code></td>
<td><p>function to apply</p></td>
</tr>
<tr class="odd">
<td><code id="chunkSpatApply_:_extent">extent</code></td>
<td><p>spatial extent of data to apply across. Defaults to the extent of
<code>x</code> if not given</p></td>
</tr>
<tr class="even">
<td><code id="chunkSpatApply_:_n_per_chunk">n_per_chunk</code></td>
<td><p>(default is 1e5) number of records to try to process per chunk.
This value can be set globally using options(gdb.nperchunk = ?)</p></td>
</tr>
<tr class="odd">
<td><code id="chunkSpatApply_:_remote_name">remote_name</code></td>
<td><p>name to assign the result in the database. Defaults to a generic
incrementing 'gdb_nnn' if not given</p></td>
</tr>
<tr class="even">
<td><code id="chunkSpatApply_:_progress">progress</code></td>
<td><p>whether to plot the progress</p></td>
</tr>
<tr class="odd">
<td><code id="chunkSpatApply_:_...">...</code></td>
<td><p>additional params to pass to Duckling object creation</p></td>
</tr>
</tbody>
</table>

### Value

dbPolygonProxy or dbPointsProxy


---
## CloseBackend

### Description

Closes pools. If specific backend\_ID(s) are given then those will be
closed. When no specific ID is given, all existing backends will be
closed.

### Usage

    closeBackend(backend_ID)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="closeBackend_:_backend_ID">backend_ID</code></td>
<td><p>hashID of backend to close (optional)</p></td>
</tr>
</tbody>
</table>


---
## ColTypes

### Description

Get the column data types of objects that inherit from `'dbData'`

### Usage

    ## S4 method for signature 'dbData'
    colTypes(x, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="colTypes_:_x">x</code></td>
<td><p>Duckling data object</p></td>
</tr>
<tr class="even">
<td><code id="colTypes_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>


---
## ComputeDBMatrix

### Description

Calculate the lazy query of a dbMatrix object and send it to the
database backend either as a temporary or permanent table.

### Usage

    computeDBMatrix(
      x,
      remote_name = "test",
      temporary = TRUE,
      overwrite = FALSE,
      ...
    )

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="computeDBMatrix_:_x">x</code></td>
<td><p>dbData object to compute from</p></td>
</tr>
<tr class="even">
<td><code id="computeDBMatrix_:_remote_name">remote_name</code></td>
<td><p>name of table to create on DB</p></td>
</tr>
<tr class="odd">
<td><code id="computeDBMatrix_:_temporary">temporary</code></td>
<td><p>(default = TRUE) whether to make a temporary table on the
DB</p></td>
</tr>
<tr class="even">
<td><code id="computeDBMatrix_:_overwrite">overwrite</code></td>
<td><p>(default = FALSE) whether to overwrite if remote_name already
exists</p></td>
</tr>
<tr class="odd">
<td><code id="computeDBMatrix_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>


---
## CPool-generic

### Description

Return the database connection pool object

### Usage

    ## S4 method for signature 'dbData'
    cPool(x)

    ## S4 replacement method for signature 'dbData'
    cPool(x) <- value

    ## S4 method for signature 'ANY'
    cPool(x)

    ## S4 replacement method for signature 'ANY'
    cPool(x) <- value

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="cPool-generic_:_x">x</code></td>
<td><p>Duckling object</p></td>
</tr>
</tbody>
</table>


---
## Create connection pool

### Description

Generate a pool object from which connection objects can be checked out.

### Usage

    create_connection_pool(drv = "duckdb::duckdb()", dbdir = ":memory:", ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="create_connection_pool_:_drv">drv</code></td>
<td><p>DB driver (default is duckdb::duckdb())</p></td>
</tr>
<tr class="even">
<td><code id="create_connection_pool_:_...">...</code></td>
<td><p>additional params to pass to pool::dbPool()</p></td>
</tr>
<tr class="odd">
<td><code id="create_connection_pool_:_dbpath">dbpath</code></td>
<td><p>path to database</p></td>
</tr>
<tr class="even">
<td><code
id="create_connection_pool_:_with_login">with_login</code></td>
<td><p>(default = FALSE) flag to check R environment variables for login
info and/or prompt for password</p></td>
</tr>
</tbody>
</table>

### Value

pool of connection objects


---
## Create dbSparseMatrix

### Description

Internal function to create a dbSparseMatrix object

### Usage

    create_dbSparseMatrix(sparse_mat, con)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="create_dbSparseMatrix_:_sparse_mat">sparse_mat</code></td>
<td><p>A sparse matrix object</p></td>
</tr>
<tr class="even">
<td><code id="create_dbSparseMatrix_:_con">con</code></td>
<td><p>A database connection object</p></td>
</tr>
</tbody>
</table>

### Value

A dbSparseMatrix object


---
## CreateBackend

### Description

Defines and creates a database connection pool to be used by the
backend. This pool object and a backendInfo object that contains details
about the

### Usage

    createBackend(
      drv = duckdb::duckdb(),
      dbdir = ":temp:",
      extension = ".duckdb",
      with_login = FALSE,
      verbose = TRUE,
      ...
    )

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="createBackend_:_drv">drv</code></td>
<td><p>database driver (default is duckdb::duckdb())</p></td>
</tr>
<tr class="even">
<td><code id="createBackend_:_dbdir">dbdir</code></td>
<td><p>directory to create a backend database</p></td>
</tr>
<tr class="odd">
<td><code id="createBackend_:_extension">extension</code></td>
<td><p>file extension (default = '.duckdb')</p></td>
</tr>
<tr class="even">
<td><code id="createBackend_:_with_login">with_login</code></td>
<td><p>(default = FALSE) whether a login is needed</p></td>
</tr>
<tr class="odd">
<td><code id="createBackend_:_verbose">verbose</code></td>
<td><p>be verbose</p></td>
</tr>
<tr class="even">
<td><code id="createBackend_:_...">...</code></td>
<td><p>additional params to pass to pool::dbPool()</p></td>
</tr>
</tbody>
</table>

### Value

invisibly returns backend ID


---
## CreateDBDataFrame

### Description

Create a dataframe with database backend

### Usage

    createDBDataFrame(
      df,
      remote_name = "df_test",
      db_path = ":temp:",
      overwrite = FALSE,
      cores = 1L,
      nlines = 10000L,
      callback = NULL,
      ...
    )

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="createDBDataFrame_:_df">df</code></td>
<td><p>object coercible to matrix or filepath to matrix data accessible
by one of the read functions. Can also be a pre-prepared tbl_sql to
compatible database table</p></td>
</tr>
<tr class="even">
<td><code id="createDBDataFrame_:_remote_name">remote_name</code></td>
<td><p>name to assign within database</p></td>
</tr>
<tr class="odd">
<td><code id="createDBDataFrame_:_db_path">db_path</code></td>
<td><p>path to database on disk</p></td>
</tr>
<tr class="even">
<td><code id="createDBDataFrame_:_overwrite">overwrite</code></td>
<td><p>whether to overwrite if table already exists in database</p></td>
</tr>
<tr class="odd">
<td><code id="createDBDataFrame_:_cores">cores</code></td>
<td><p>number of cores to use if reading into database</p></td>
</tr>
<tr class="even">
<td><code id="createDBDataFrame_:_nlines">nlines</code></td>
<td><p>number of lines to read per chunk if reading into
database</p></td>
</tr>
<tr class="odd">
<td><code id="createDBDataFrame_:_callback">callback</code></td>
<td><p>callback functions to apply to each data chunk before it is sent
to the database backend</p></td>
</tr>
<tr class="even">
<td><code id="createDBDataFrame_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>

### Details

Information is only read into the database during this process. Based on
the `remote_name` and `db_path` a lazy connection is then made
downstream during `dbData` initialization and appended to the object. If
a dplyr tbl is provided as pre-made input then it is evaluated for
whether it is a `tbl_Pool` and whether the table exists within the
specified backend then directly passed downstream.


---
## CreateDBMatrix

### Description

Create an S4 dbMatrix object that has a triplet format under the hood
(ijx). The data for the matrix is either written to a specified database
file or could also be read in from files on disk

### Usage

    createDBMatrix(
      matrix,
      remote_name = "mat_test",
      db_path = ":temp:",
      overwrite = FALSE,
      cores = 1L,
      nlines = 10000L,
      callback = callback_formatIJX(),
      custom_table_fields = fields_preset$dbMatrix_ijx,
      dims,
      dim_names,
      ...
    )

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="createDBMatrix_:_matrix">matrix</code></td>
<td><p>object coercible to matrix or filepath to matrix data accessible
by one of the read functions. Can also be a pre-prepared tbl_sql to
compatible database table</p></td>
</tr>
<tr class="even">
<td><code id="createDBMatrix_:_remote_name">remote_name</code></td>
<td><p>name to assign within database</p></td>
</tr>
<tr class="odd">
<td><code id="createDBMatrix_:_db_path">db_path</code></td>
<td><p>path to database on disk</p></td>
</tr>
<tr class="even">
<td><code id="createDBMatrix_:_overwrite">overwrite</code></td>
<td><p>whether to overwrite if table already exists in database</p></td>
</tr>
<tr class="odd">
<td><code id="createDBMatrix_:_cores">cores</code></td>
<td><p>number of cores to use if reading into database</p></td>
</tr>
<tr class="even">
<td><code id="createDBMatrix_:_nlines">nlines</code></td>
<td><p>number of lines to read per chunk if reading into
database</p></td>
</tr>
<tr class="odd">
<td><code id="createDBMatrix_:_callback">callback</code></td>
<td><p>callback functions to apply to each data chunk before it is sent
to the database backend</p></td>
</tr>
<tr class="even">
<td><code id="createDBMatrix_:_dims">dims</code></td>
<td><p>dimensions of the matrix (optional)</p></td>
</tr>
<tr class="odd">
<td><code id="createDBMatrix_:_dim_names">dim_names</code></td>
<td><p>list of rownames and colnames of the matrix (optional)</p></td>
</tr>
<tr class="even">
<td><code id="createDBMatrix_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>

### Details

Information is only read into the database during this process. Based on
the `remote_name` and `db_path` a lazy connection is then made
downstream during `dbData` initialization and appended to the object. If
a dplyr tbl is provided as pre-made input then it is evaluated for
whether it is a `tbl_Pool` and whether the table exists within the
specified backend then directly passed downstream.


---
## CreateDBPolygonProxy

### Description

Create an S4 dbPolygonProxy object that is composed of two database
tables. One table hold geometry information while the other holds
attribute information.

### Usage

    createDBPolygonProxy(
      SpatVector,
      remote_name = "poly_test",
      db_path = ":temp:",
      id_col = "poly_ID",
      xy_col = c("x", "y"),
      extent = NULL,
      overwrite = FALSE,
      chunk_size = 10000L,
      callback = NULL,
      custom_table_fields = fields_preset$dbPoly_geom,
      custom_table_fields_attr = NULL,
      attributes = NULL,
      ...
    )

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="createDBPolygonProxy_:_SpatVector">SpatVector</code></td>
<td><p>object coercible to SpatVector or filepath to spatial data
readable by <code>vect</code></p></td>
</tr>
<tr class="even">
<td><code
id="createDBPolygonProxy_:_remote_name">remote_name</code></td>
<td><p>name of remote table on database backend</p></td>
</tr>
<tr class="odd">
<td><code id="createDBPolygonProxy_:_db_path">db_path</code></td>
<td><p>filepath to the database backend</p></td>
</tr>
<tr class="even">
<td><code id="createDBPolygonProxy_:_id_col">id_col</code></td>
<td><p>column in data to read in that contains the polygon id
information</p></td>
</tr>
<tr class="odd">
<td><code id="createDBPolygonProxy_:_xy_col">xy_col</code></td>
<td><p>columns in data to read in that contain the x and y vertex
info</p></td>
</tr>
<tr class="even">
<td><code id="createDBPolygonProxy_:_extent">extent</code></td>
<td><p>terra SpatExtent (optional) that can be used to subset the data
to read in before it is saved to database</p></td>
</tr>
<tr class="odd">
<td><code id="createDBPolygonProxy_:_overwrite">overwrite</code></td>
<td><p>whether to overwrite if <code>remote_name</code> already exists
on the database</p></td>
</tr>
<tr class="even">
<td><code id="createDBPolygonProxy_:_chunk_size">chunk_size</code></td>
<td><p>the number of polygons to read in per chunk read</p></td>
</tr>
<tr class="odd">
<td><code id="createDBPolygonProxy_:_callback">callback</code></td>
<td><p>data formatting and manipulations to perform chunkwise before the
data is saved to database. Instructions should be provided as a function
that takes an input of a data.table and returns a data.table</p></td>
</tr>
<tr class="even">
<td><code
id="createDBPolygonProxy_:_custom_table_fields">custom_table_fields</code></td>
<td><p>(optional) custom table field SQL settings to use during table
creation for the spatial geometry table</p></td>
</tr>
<tr class="odd">
<td><code
id="createDBPolygonProxy_:_custom_table_fields_attr">custom_table_fields_attr</code></td>
<td><p>(optional) custom table field SQL settings to use during table
creation for the attributes table</p></td>
</tr>
<tr class="even">
<td><code id="createDBPolygonProxy_:_attributes">attributes</code></td>
<td><p>(optional) a <code>tbl_sql</code> connected to the database
backend that contains the associated attributes table. Only used if a
pre-made <code>tbl_sql</code> is also provided to
<code>SpatVector</code> param</p></td>
</tr>
</tbody>
</table>

### Details

Information is only read into the database during this process. Based on
the `remote_name` and `db_path` a lazy connection is then made
downstream during dbData intialization and appended to the object. If
the data already exists within the database backend then it is entirely
permissible to omit the `SpatVector` param.


---
## CreateTableBE

### Description

Create a table in the backend. Adds some additional functionality on top
of what is expected from `dbCreateTable`. A primary key can be be
provided at table creation. Partial inputs to define the table fields
are also possible, where the default data types and schema will be
generated based on a passed in data.frame, but additional information
provided as a named list to `fields_custom` param will override the
relevant generated default.

### Usage

    createTableBE(
      conn,
      name,
      fields_df,
      fields_custom = NULL,
      pk = NULL,
      ...,
      row.names = NULL,
      temporary = FALSE
    )

### Arguments

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<tbody>
<tr class="odd">
<td><code id="createTableBE_:_conn">conn</code></td>
<td><p>A database connection</p></td>
</tr>
<tr class="even">
<td><code id="createTableBE_:_name">name</code></td>
<td><p>table name</p></td>
</tr>
<tr class="odd">
<td><code id="createTableBE_:_fields_df">fields_df</code></td>
<td><p>data frame from which to determine default data types</p></td>
</tr>
<tr class="even">
<td><code id="createTableBE_:_fields_custom">fields_custom</code></td>
<td><p>named character vector of columns and manually assigned data
types along with any other in-line constraints</p></td>
</tr>
<tr class="odd">
<td><code id="createTableBE_:_pk">pk</code></td>
<td><p>character. Which column(s) to select as the primary key</p></td>
</tr>
<tr class="even">
<td><code id="createTableBE_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
<tr class="odd">
<td><code id="createTableBE_:_row.names">row.names</code></td>
<td><p>Either TRUE, FALSE, NA or a string.<br />
If TRUE, always translate row names to a column called "row_names". If
FALSE, never translate row names. If NA, translate rownames only if
they're a character vector.<br />
A string is equivalent to TRUE, but allows you to override the default
name.<br />
For backward compatibility, NULL is equivalent to FALSE.</p></td>
</tr>
<tr class="even">
<td><code id="createTableBE_:_temporary">temporary</code></td>
<td><p>If TRUE, will generate a temporary table statement.</p></td>
</tr>
</tbody>
</table>


---
## DbDataFrame-class

### Description

Representation of dataframes using an on-disk database. Each object is
used as a connection to a single table that exists within the database.

### Slots

`data`  
dplyr tbl that represents the database data

`hash`  
unique hash ID for backend

`remote_name`  
name of table within database that contains the data

`key`  
column to set as key for ordering and subsetting on i


---
## DbDenseMatrix-class

### Description

Representation of dense matrices using an on-disk database. Inherits
from dbMatrix.

### Slots

`data`  
An ijx matrix with zeros


---
## DbMatrix-class

### Description

Representation of triplet matrices using an on-disk database. Each
object is used as a connection to a single table that exists within the
database.

### Slots

`data`  
dplyr tbl that represents the database data

`hash`  
unique hash ID for backend

`remote_name`  
name of table within database that contains the data

`path`  
path to database on-disk file

`dim_names`  
row \[1\] and col \[2\] names

`dims`  
dimensions of the matrix


---
## DbMemoryLimit

### Description

Get and set DB memory limits

### Usage

    dbMemoryLimit(x, limit, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="dbMemoryLimit_:_x">x</code></td>
<td><p>backend_ID or pool object</p></td>
</tr>
<tr class="even">
<td><code id="dbMemoryLimit_:_limit">limit</code></td>
<td><p>character. Memory limit to use with units included (for example
'10GB'). If missing, will get the current setting. If 'RESET' will reset
to default</p></td>
</tr>
<tr class="odd">
<td><code id="dbMemoryLimit_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>


---
## Dbms-generic

### Description

Get the database management system (dbms) of an object

### Usage

    ## S4 method for signature 'dbData'
    dbms(x, ...)

    ## S4 method for signature 'character'
    dbms(x, ...)

    ## S4 method for signature 'Pool'
    dbms(x, ...)

    ## S4 method for signature 'DBIConnection'
    dbms(x, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="dbms-generic_:_x">x</code></td>
<td><p>A connection pool object, DBI connection, or dbData</p></td>
</tr>
<tr class="even">
<td><code id="dbms-generic_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>


---
## DbPointsProxy-class

### Description

Representation of point information using an on-disk database. Intended
to be used to store information that can be pulled into terra point
SpatVectors

### Slots

`n_points`  
number of points

`feat_ID`  
feature IDs

`extent`  
extent of points

`poly_filter`  
polygon SpatVector that is used to filter values on read-in


---
## DbPolygonProxy-class

### Description

Representation of polygon information using an on-disk database.
Intended to be used to store information that can be pulled into terra
polygon SpatVectors

### Slots

`data`  
lazy table containing geometry information with columns geom, part, x,
y, and hole

`attributes`  
dbDataFrame of attributes information, one of which (usually the first)
being 'ID' that can be joined/matched against the 'geom' values in
`attributes`

`n_poly`  
number of polygons

`poly_ID`  
polygon IDs

`extent`  
extent of polygons

`poly_filter`  
polygon SpatVector that is used to filter values on read-in


---
## DbSettings

### Description

Get and set database settings

### Usage

    ## S4 method for signature 'ANY,character,missing'
    dbSettings(x, setting, value, ...)

    ## S4 method for signature 'ANY,character,ANY'
    dbSettings(x, setting, value, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="dbSettings_:_x">x</code></td>
<td><p>backend_ID or pool object</p></td>
</tr>
<tr class="even">
<td><code id="dbSettings_:_setting">setting</code></td>
<td><p>character. Setting to get or set</p></td>
</tr>
<tr class="odd">
<td><code id="dbSettings_:_value">value</code></td>
<td><p>if missing, will retrieve the setting. If provided, will attempt
to set the new value. If 'RESET' will reset the value to
default</p></td>
</tr>
<tr class="even">
<td><code id="dbSettings_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>


---
## DbSparseMatrix-class

### Description

Representation of sparse matrices using an on-disk database. Inherits
from dbMatrix.

### Slots

`data`  
An ijx matrix without zeros


---
## DbThreads

### Description

Set and get number of threads to use in database backend

### Usage

    dbThreads(x, threads, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="dbThreads_:_x">x</code></td>
<td><p>backend ID or pool object of backend</p></td>
</tr>
<tr class="even">
<td><code id="dbThreads_:_threads">threads</code></td>
<td><p>numeric or integer. Number of threads to use. If missing, will
get the current setting. If 'RESET' will reset to default.</p></td>
</tr>
<tr class="odd">
<td><code id="dbThreads_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>


---
## Desparse to grid bin data

### Description

Desparsify binned data and assign to rasterized locations

### Usage

    desparse_to_grid_bin_data(data, px_x, px_y)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="desparse_to_grid_bin_data_:_data">data</code></td>
<td><p>data.frame</p></td>
</tr>
<tr class="even">
<td><code id="desparse_to_grid_bin_data_:_extent">extent</code></td>
<td><p>terra extent in which to plot</p></td>
</tr>
<tr class="odd">
<td><code
id="desparse_to_grid_bin_data_:_resolution">resolution</code></td>
<td><p>min number of tiles to collect</p></td>
</tr>
</tbody>
</table>


---
## Disconnect

### Description

Disconnects from a database. Closes both the connection and driver
objects by default

### Usage

    ## S4 method for signature 'dbData'
    disconnect(x, shutdown = TRUE)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="disconnect_:_x">x</code></td>
<td><p>Duckling object</p></td>
</tr>
</tbody>
</table>


---
## DropTableBE

### Description

Drop a table from the database

### Usage

    dropTableBE(conn, remote_name)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="dropTableBE_:_conn">conn</code></td>
<td><p>connection object or pool</p></td>
</tr>
<tr class="even">
<td><code id="dropTableBE_:_remote_name">remote_name</code></td>
<td><p>name of table to drop</p></td>
</tr>
</tbody>
</table>


---
## ExistingHashIDs

### Description

Get all existing backend hash IDs

### Usage

    existingHashIDs()


---
## ExistsTableBE

### Description

Whether a particular table exists in a connection

### Usage

    ## S4 method for signature 'dbData,character'
    existsTableBE(x, remote_name, ...)

    ## S4 method for signature 'ANY,character'
    existsTableBE(x, remote_name, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="existsTableBE_:_x">x</code></td>
<td><p>Duckling object or DB connection</p></td>
</tr>
<tr class="even">
<td><code id="existsTableBE_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>


---
## Extent calculate

### Description

Calculate dbSpatProxyData spatial extent

### Usage

    ## S4 method for signature 'dbSpatProxyData'
    extent_calculate(x, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="extent_calculate_:_x">x</code></td>
<td><p>a dbSpatProxyData object</p></td>
</tr>
</tbody>
</table>

### Value

terra SpatExtent

### See Also

Other Extent processing functions: `extent_filter()`


---
## Extent filter

### Description

Filter database-backed spatial data for only those records that fall
within a spatial `extent` as given by a terra `SpatExtent` object. This
selection

### Usage

    ## S4 method for signature 'dbPolygonProxy,SpatExtent,logical'
    extent_filter(x, extent, include, method = c("all", "mean"), ...)

    ## S4 method for signature 'dbPointsProxy,SpatExtent,logical'
    extent_filter(x, extent, include, ...)

    ## S4 method for signature 'ANY,SpatExtent,missing'
    extent_filter(x, extent, include, ...)

    ## S4 method for signature 'ANY,SpatExtent,logical'
    extent_filter(x, extent, include, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="extent_filter_:_x">x</code></td>
<td><p>dbSpatProxyData</p></td>
</tr>
<tr class="even">
<td><code id="extent_filter_:_extent">extent</code></td>
<td><p>SpatExtent defining a spatial region to select</p></td>
</tr>
<tr class="odd">
<td><code id="extent_filter_:_include">include</code></td>
<td><p>logical vector of the form c(bottom, left, top, right) which
determines whether the specified extent bound should be inclusive of the
bound value itself. (ie greater/less than OR equal to (default) vs only
greater/less than)</p></td>
</tr>
<tr class="even">
<td><code id="extent_filter_:_method">method</code></td>
<td><p>character. Method of selection. 'mean' (default) selects a
polygon if the mean point of all vertex coordinates falls within the
<code>extent</code>. 'all' selects a polygon if ANY of its vertices fall
within the <code>extent</code>.</p></td>
</tr>
<tr class="odd">
<td><code id="extent_filter_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>

### See Also

Other Extent processing functions: `extent_calculate()`


---
## Fields preset

### Description

Fields presets to use with DBI::dbCreateTable() to set up remote tables
and their constraints

### Usage

    fields_preset

### Format

An object of class `list` of length 2.

### Details

Package presets for DB table fields that can be easily accessed


---
## File extension

### Description

Get file extension(s)

### Usage

    file_extension(file)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="file_extension_:_file">file</code></td>
<td><p>filepath</p></td>
</tr>
</tbody>
</table>


---
## Filter dbspat

### Description

Internal function to abstract away the differences in handling of
filtering data for dbPolygonProxy and dbPointsProxy. This is because the
geometry and values (attributes) are separated into two tables for
dbPolygonProxy but are present in a single table for dbPointsProxy. This
function accepts `dbSpatProxyData` as input, but the function passed to
by\_geom or by\_value should be defined for the internal `tbl_sql`.

### Usage

    ## S4 method for signature 'dbPolygonProxy,'function',missing'
    filter_dbspat(x, by_geom = NULL, by_value = NULL, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="filter_dbspat_:_x">x</code></td>
<td><p>dbSpatProxyData</p></td>
</tr>
<tr class="even">
<td><code id="filter_dbspat_:_by_geom">by_geom</code>, <code
id="filter_dbspat_:_by_value">by_value</code></td>
<td><p>dplyr/dbplyr function to manipulate the data with across either
the geometry OR value data.</p></td>
</tr>
</tbody>
</table>

### Examples

```r
dbPoly <- simulate_dbPolygonProxy()
dbPoly_filtered <- filter_dbspat(x = dbpoly,
 by_value = function(dbspd) {
   dplyr::filter(dbspd, poly_ID == '101161259912191124732236989250178928032')
 })
dbPoly_filtered
dbPoly_filtered <- filter_dbspat(x = dbpoly,
 by_geom = function(dbspd) {
   dbspd %>% dplyr::filter(x > 6500)
 })
dbPoly_filtered
```


---
## Get dim n chunks

### Description

Algorithm to determine how to divide up a provided extent into at least
`n` different chunks. The chunks are arranged so as to prefer being as
square as posssible with the provided dimensions and minimum n chunks.

### Usage

    get_dim_n_chunks(n, e)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="get_dim_n_chunks_:_n">n</code></td>
<td><p>minimum n chunks</p></td>
</tr>
<tr class="even">
<td><code id="get_dim_n_chunks_:_e">e</code></td>
<td><p>selection extent</p></td>
</tr>
</tbody>
</table>

### Value

numeric vector of x and y stops needed

### See Also

`chunk_plan`

### Examples

```r
## Not run: 
e <- terra::ext(0, 100, 0, 100)
get_dim_n_chunk(n = 5, e = e)

## End(Not run)
```


---
## Get full table name quoted

### Description

Get the full name of a table. This information is then quoted to protect
against SQL injections. Either param x or hash must be given

### Usage

    get_full_table_name_quoted(conn, remote_name)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="get_full_table_name_quoted_:_conn">conn</code></td>
<td><p>a hashID, DBI connection object, or pool object</p></td>
</tr>
<tr class="even">
<td><code
id="get_full_table_name_quoted_:_remote_name">remote_name</code></td>
<td><p>name of table within DB</p></td>
</tr>
</tbody>
</table>

### Value

return the full table name


---
## GetBackendEnv

### Description

Get the backend details environment

### Usage

    getBackendEnv()


---
## GetBackendID

### Description

Get the backend hash ID from the database path

### Usage

    getBackendID(path = ":temp:", extension = ".duckdb")

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="getBackendID_:_path">path</code></td>
<td><p>directory path to the database. Accepts :memory: and :temp:
inputs as well</p></td>
</tr>
<tr class="even">
<td><code id="getBackendID_:_extension">extension</code></td>
<td><p>file extension of database backend (default = '.duckdb')</p></td>
</tr>
</tbody>
</table>


---
## GetBackendPool

### Description

Get backend information. `getBackendPool` gets the associated connection
pool object. `getBackendInfo` gets the backendInfo object that contains
all connection details needed to regenerate another connection

### Usage

    getBackendPool(backend_ID)

    getBackendInfo(backend_ID)

    getBackendConn(backend_ID)

    getBackendPath(backend_ID)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="getBackendPool_:_backend_ID">backend_ID</code></td>
<td><p>hashID generated from the dbpath (xxhash64) used as unique ID for
the backend</p></td>
</tr>
</tbody>
</table>

### Functions

-   `getBackendInfo()`: get backendInfo object containing connection
    details

-   `getBackendConn()`: Get a DBI connection object from pool. Provided
    for convenience. Must be returned using pool::poolReturn() after
    use.

-   `getBackendPath()`: get filepath of backend


---
## GetDBPath

### Description

Get the full normalized filepath of a Giotto backend database.
Additionally passing path = ':memory:' will directly return ':memory:'
and ':temp:' will will have the function check tempdir() for the
backend.

### Usage

    getDBPath(path = ":temp:", extension = ".duckdb")

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="getDBPath_:_path">path</code></td>
<td><p>directory path in which to place the backend</p></td>
</tr>
<tr class="even">
<td><code id="getDBPath_:_extension">extension</code></td>
<td><p>file extension of the backend (default is .duckdb)</p></td>
</tr>
</tbody>
</table>


---
## Hidden aliases

### Description

Get cell attributes from a dbSpatProxyData Values are only returned as a
`dbDataFrame` from dbSpatProxyData

### Usage

    ## S4 method for signature 'dbMatrix'
    Math(x)

    ## S4 method for signature 'dbMatrix'
    as.matrix(x, ...)

    ## S4 method for signature 'dbPolygonProxy'
    as.polygons(x, ...)

    ## S4 method for signature 'dbPointsProxy'
    as.points(x, ...)

    ## S4 method for signature 'dbData,missing,missing,missing'
    x[i, j]

    ## S4 replacement method for signature 'dbData,missing,missing,ANY'
    x[i, j] <- value

    ## S4 method for signature 'dbMatrix,gdbIndex,missing,missing'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbMatrix,missing,gdbIndex,missing'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbMatrix,gdbIndex,gdbIndex,missing'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbDataFrame,gdbIndex,missing,ANY'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbDataFrame,missing,gdbIndex,ANY'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbDataFrame,gdbIndex,gdbIndex,ANY'
    x[i, j, ..., drop = FALSE]

    ## S4 method for signature 'dbDataFrame,missing,missing,missing'
    x[i, j]

    ## S4 replacement method for signature 'dbDataFrame,missing,missing,ANY'
    x[i, j] <- value

    ## S4 method for signature 'dbPolygonProxy,character,missing,ANY'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbPointsProxy,character,missing,ANY'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbPointsProxy,numeric,missing,ANY'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbPointsProxy,logical,missing,ANY'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbPolygonProxy,missing,character,ANY'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbPointsProxy,missing,character,ANY'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbPointsProxy,missing,numeric,ANY'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbPointsProxy,missing,logical,ANY'
    x[i, j, ..., drop = TRUE]

    ## S4 method for signature 'dbPointsProxy,gdbIndexNonChar,gdbIndex,ANY'
    x[i, j, ..., drop = FALSE]

    ## S4 method for signature 'dbPolygonProxy'
    values(x, ...)

    ## S4 method for signature 'dbPointsProxy'
    values(x, ...)

    ## S4 method for signature 'dbSpatProxyData,SpatExtent'
    crop(x, y, ...)

    ## S4 method for signature 'dbDataFrame'
    names(x)

    ## S4 replacement method for signature 'dbDataFrame,gdbIndex'
    names(x) <- value

    ## S4 method for signature 'dbPolygonProxy'
    names(x)

    ## S4 replacement method for signature 'dbPolygonProxy,gdbIndex'
    names(x) <- value

    ## S4 method for signature 'dbPointsProxy'
    names(x)

    ## S4 replacement method for signature 'dbPointsProxy,gdbIndex'
    names(x) <- value

    ## S4 method for signature 'dbData'
    rownames(x)

    ## S4 method for signature 'dbMatrix'
    rownames(x)

    ## S4 replacement method for signature 'dbMatrix'
    rownames(x) <- value

    ## S4 method for signature 'dbData'
    colnames(x)

    ## S4 method for signature 'dbMatrix'
    colnames(x)

    ## S4 replacement method for signature 'dbMatrix,ANY'
    colnames(x) <- value

    ## S4 replacement method for signature 'dbDataFrame,gdbIndex'
    colnames(x) <- value

    ## S4 method for signature 'dbMatrix'
    dimnames(x)

    ## S4 replacement method for signature 'dbMatrix,list'
    dimnames(x) <- value

    ## S4 method for signature 'dbDataFrame'
    dimnames(x)

    ## S4 replacement method for signature 'dbDataFrame,list'
    dimnames(x) <- value

    ## S4 method for signature 'dbMatrix,ANY'
    Arith(e1, e2)

    ## S4 method for signature 'ANY,dbMatrix'
    Arith(e1, e2)

    ## S4 method for signature 'dbMatrix,ANY'
    Ops(e1, e2)

    ## S4 method for signature 'ANY,dbMatrix'
    Ops(e1, e2)

    ## S4 method for signature 'dbMatrix,dbMatrix'
    Arith(e1, e2)

    ## S4 method for signature 'dbMatrix,dbMatrix'
    Ops(e1, e2)

    ## S4 method for signature 'dbMatrix'
    rowSums(x, na.rm = FALSE, dims = 1, ...)

    ## S4 method for signature 'dbMatrix'
    colSums(x, na.rm = FALSE, dims = 1, ...)

    ## S4 method for signature 'dbMatrix'
    rowMeans(x, na.rm = FALSE, dims = 1, ...)

    ## S4 method for signature 'dbMatrix'
    colMeans(x, na.rm = FALSE, dims = 1, ...)

    ## S4 method for signature 'dbMatrix'
    colSds(x, ...)

    ## S4 method for signature 'dbMatrix'
    rowSds(x, ...)

    ## S4 method for signature 'dbMatrix'
    t(x)

    ## S4 method for signature 'dbPointsProxy'
    t(x)

    ## S4 method for signature 'dbPolygonProxy'
    t(x)

    ## S4 method for signature 'dbMatrix'
    mean(x, ...)

    ## S4 method for signature 'dbDataFrame'
    nrow(x)

    ## S4 method for signature 'dbPointsProxy'
    nrow(x)

    ## S4 method for signature 'dbPolygonProxy'
    nrow(x)

    ## S4 method for signature 'dbMatrix'
    ncol(x)

    ## S4 method for signature 'dbDataFrame'
    ncol(x)

    ## S4 method for signature 'dbPointsProxy'
    ncol(x)

    ## S4 method for signature 'dbPolygonProxy'
    ncol(x)

    ## S4 method for signature 'dbDataFrame'
    ncol(x)

    ## S4 method for signature 'dbData'
    dim(x)

    ## S4 method for signature 'dbMatrix'
    dim(x)

    ## S4 method for signature 'dbPointsProxy'
    dim(x)

    ## S4 method for signature 'dbPolygonProxy'
    dim(x)

    ## S4 method for signature 'dbSpatProxyData'
    length(x)

    ## S4 method for signature 'dbSpatProxyData'
    ext(x, ...)

    ## S4 replacement method for signature 'dbSpatProxyData,SpatExtent'
    ext(x) <- value

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="Math+2CdbMatrix-method_:_x">x</code></td>
<td><p>object to crop</p></td>
</tr>
<tr class="even">
<td><code id="Math+2CdbMatrix-method_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
<tr class="odd">
<td><code id="Math+2CdbMatrix-method_:_y">y</code></td>
<td><p>object to crop with</p></td>
</tr>
</tbody>
</table>

### Value

dbSpatProxyData


---
## Is init-generic

### Description

Determine if dbData object is initialized

### Usage

    ## S4 method for signature 'dbData'
    is_init(x, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="is_init-generic_:_x">x</code></td>
<td><p>dbData object</p></td>
</tr>
</tbody>
</table>


---
## KeyCol

### Description

Set a column as key in order to sort and subset by rows with
dbDataFrame. More than one column can be set at the same time, which
will sort the table by multiple columns (ascending), starting with the
first specified.

### Usage

    ## S4 method for signature 'dbDataFrame'
    keyCol(x, ...)

    ## S4 replacement method for signature 'dbDataFrame,character'
    keyCol(x, ...) <- value

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="keyCol_:_x">x</code></td>
<td><p>dbDataFrame</p></td>
</tr>
<tr class="even">
<td><code id="keyCol_:_value">value</code></td>
<td><p>character. Column to set as key</p></td>
</tr>
</tbody>
</table>

### Examples

```r
d <- simulate_dbDataFrame()
keyCol(d) <- c('Sepal.Length', 'Sepal.Width')
d[1:6,]
```


---
## ListTablesBE

### Description

List the tables in a connection

### Usage

    ## S4 method for signature 'dbData'
    listTablesBE(x, ...)

    ## S4 method for signature 'character'
    listTablesBE(x, ...)

    ## S4 method for signature 'ANY'
    listTablesBE(x, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="listTablesBE_:_x">x</code></td>
<td><p>Duckling object or DB connection</p></td>
</tr>
<tr class="even">
<td><code id="listTablesBE_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>


---
## Nrow

### Description

`nrow` and `ncol` return the number of rows or columns present in `x`.

### Usage

    ## S4 method for signature 'dbMatrix'
    nrow(x)


---
## Pipe

### Description

See `magrittr::%>%` for details.

### Usage

    lhs %>% rhs


---
## Preview chunk plan

### Description

Plots the output from `chunk_plan` as a set of polygons to preview. Can
be useful for debugging. Invisibly returns the planned chunks as a
SpatVector of polygons

### Usage

    preview_chunk_plan(extent_list, mode = c("poly", "bound"))

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="preview_chunk_plan_:_extent_list">extent_list</code></td>
<td><p>list of extents from <code>chunk_plan</code></p></td>
</tr>
</tbody>
</table>


---
## PrimaryKey

### Description

Show if table has any primary keys

### Usage

    primaryKey(conn, remote_name)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="primaryKey_:_conn">conn</code></td>
<td><p>hashID of backend, DBI connection, or pool</p></td>
</tr>
<tr class="even">
<td><code id="primaryKey_:_remote_name">remote_name</code></td>
<td><p>name of table on DB</p></td>
</tr>
</tbody>
</table>


---
## QueryStack-generic

### Description

Get and set the queryStack for a db backend object

### Usage

    ## S4 method for signature 'dbData'
    queryStack(x)

    ## S4 method for signature 'ANY'
    queryStack(x)

    ## S4 replacement method for signature 'dbData'
    queryStack(x) <- value

    ## S4 replacement method for signature 'ANY'
    queryStack(x) <- value

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="queryStack-generic_:_x">x</code></td>
<td><p>Duckling object</p></td>
</tr>
</tbody>
</table>


---
## Raster bin data

### Description

Bin database values in xy

### Usage

    raster_bin_data(data, extent, resolution = 50000)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="raster_bin_data_:_data">data</code></td>
<td><p>tbl_Pool to use</p></td>
</tr>
<tr class="even">
<td><code id="raster_bin_data_:_extent">extent</code></td>
<td><p>terra extent in which to plot</p></td>
</tr>
<tr class="odd">
<td><code id="raster_bin_data_:_resolution">resolution</code></td>
<td><p>min number of tiles to collect</p></td>
</tr>
</tbody>
</table>

### Value

matrix of rasterized point information


---
## ReadMatrixDT

### Description

Function to read a matrix in from flat file via R and data.table

### Usage

    readMatrixDT(path, cores = 1L, transpose = FALSE)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="readMatrixDT_:_path">path</code></td>
<td><p>path to the matrix file</p></td>
</tr>
<tr class="even">
<td><code id="readMatrixDT_:_cores">cores</code></td>
<td><p>number of cores to use</p></td>
</tr>
<tr class="odd">
<td><code id="readMatrixDT_:_transpose">transpose</code></td>
<td><p>transpose matrix</p></td>
</tr>
</tbody>
</table>

### Details

The matrix needs to have both unique column names and row names

### Value

matrix


---
## Reconnect

### Description

Checks if the object still has a valid connection. If it does then it
returns the object without modification. If it does need reconnection
then a copy of the associated active connection pool object is pulled
from .DB\_ENV and if that is also closed, an error is thrown asking for
a reconnectBackend() run.

Also ensures that the object contains a reference to a valid table
within the database. Runs at the beginning of most function calls
involving dbData object queries.

### Usage

    ## S4 method for signature 'dbData'
    reconnect(x)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="reconnect_:_x">x</code></td>
<td><p>Duckling object</p></td>
</tr>
</tbody>
</table>


---
## ReconnectBackend-generic

### Description

Reconnect Duckling backend

### Usage

    ## S4 method for signature 'backendInfo'
    reconnectBackend(x, with_login = FALSE, verbose = TRUE, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="reconnectBackend-generic_:_x">x</code></td>
<td><p>backendInfo object</p></td>
</tr>
<tr class="even">
<td><code
id="reconnectBackend-generic_:_with_login">with_login</code></td>
<td><p>(default = FALSE) flag to check R environment variables</p></td>
</tr>
<tr class="odd">
<td><code id="reconnectBackend-generic_:_verbose">verbose</code></td>
<td><p>be verbose</p></td>
</tr>
<tr class="even">
<td><code id="reconnectBackend-generic_:_...">...</code></td>
<td><p>additional params to pass for login info and/or prompt for
password</p></td>
</tr>
</tbody>
</table>


---
## RemoteName-generic

### Description

Returns the table name within the database the the object acts as a link
to

### Usage

    ## S4 method for signature 'dbData'
    remoteName(x)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="remoteName-generic_:_x">x</code></td>
<td><p>Duckling object</p></td>
</tr>
</tbody>
</table>


---
## Render image

### Description

internal function for rasterized rendering from matrix data

### Usage

    render_image(mat, cols = c("#002222", "white", "#800020"), axes = FALSE)


---
## Simplify poly as point

### Description

Internal. Calculate simple representative spatial locations for
polygons. Depending on `method` param, finds the polygon vertex that is
the mean or at the lower left, lower right, upper left, or upper right.
All methods are roughly equivalent in speed. Note that this is NOT the
polygon centroid.

### Usage

    simplify_poly_as_point(
      x,
      method = c("mean", "lowL", "lowR", "upL", "upR"),
      ...
    )

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="simplify_poly_as_point_:_x">x</code></td>
<td><p>tbl_Pool (usually from a dbPolygonProxy object)</p></td>
</tr>
<tr class="even">
<td><code id="simplify_poly_as_point_:_method">method</code></td>
<td><p>one of 'mean', 'lowL', 'lowR', 'upL', 'upR'</p></td>
</tr>
<tr class="odd">
<td><code id="simplify_poly_as_point_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>

### Details

Generate a representative point for all the polygons in a
dbPolygonProxy. This facilitates spatial selection of the polygons as
there are fewer points to keep track of, making it less computationally
intensive and less ambiguous whether a polygon should be selected by a
specific selection if parts of it fall in multiple selections.

### Value

tbl\_Pool of xy coordinates


---
## Simulate objects

### Description

Create simulated Duckling objects in memory from pre-prepared data.
Useful for testing purposes and examples.

### Usage

    simulate_duckdb(data = datasets::iris, name = "test", p = NULL)

    simulate_dbDataFrame(data = NULL, name = "df_test", key = NA_character_)

    simulate_dbPointsProxy(data = NULL)

    simulate_dbPolygonProxy(data = NULL)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="simulate_objects_:_data">data</code></td>
<td><p>data to use</p></td>
</tr>
<tr class="even">
<td><code id="simulate_objects_:_name">name</code></td>
<td><p>remote name of table on database to set</p></td>
</tr>
</tbody>
</table>

### Functions

-   `simulate_duckdb()`: Simulate a duckdb connection dplyr tbl\_Pool in
    memory

-   `simulate_dbDataFrame()`: Simulate a dbDataFrame in memory

-   `simulate_dbPointsProxy()`: Simulate a dbPointsProxy in memory

-   `simulate_dbPolygonProxy()`: Simulate a dbPolygonProxy in memory


---
## Sql create table

### Description

internal function that allows the addition of a primary key line to the
table generation sql from sqlCreateTable

### Usage

    sql_create_pk_table(
      con,
      table,
      fields,
      pk,
      row.names = NA,
      temporary = FALSE,
      ...
    )

### Arguments

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<tbody>
<tr class="odd">
<td><code id="sql_create_table_:_con">con</code></td>
<td><p>A database connection</p></td>
</tr>
<tr class="even">
<td><code id="sql_create_table_:_table">table</code></td>
<td><p>table name</p></td>
</tr>
<tr class="odd">
<td><code id="sql_create_table_:_fields">fields</code></td>
<td><p>a named character vector or a data frame</p></td>
</tr>
<tr class="even">
<td><code id="sql_create_table_:_pk">pk</code></td>
<td><p>character. Which columns to select as the primary key</p></td>
</tr>
<tr class="odd">
<td><code id="sql_create_table_:_row.names">row.names</code></td>
<td><p>Either TRUE, FALSE, NA or a string.<br />
If TRUE, always translate row names to a column called "row_names". If
FALSE, never translate row names. If NA, translate rownames only if
they're a character vector.<br />
A string is equivalent to TRUE, but allows you to override the default
name.<br />
For backward compatibility, NULL is equivalent to FALSE.</p></td>
</tr>
<tr class="even">
<td><code id="sql_create_table_:_temporary">temporary</code></td>
<td><p>If TRUE, will generate a temporary table statement.</p></td>
</tr>
<tr class="odd">
<td><code id="sql_create_table_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>


---
## Sql query-generic

### Description

Query a database object using a manually prepared SQL statement.
Integrates into dplyr chains. More general purpose than sql\_gen\*
functions

### Usage

    ## S4 method for signature 'dbData,character'
    sql_query(x, statement, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="sql_query-generic_:_x">x</code></td>
<td><p>a database object</p></td>
</tr>
<tr class="even">
<td><code id="sql_query-generic_:_statement">statement</code></td>
<td><p>query statement in SQL where the FROM field should be
:data:</p></td>
</tr>
<tr class="odd">
<td><code id="sql_query-generic_:_...">...</code></td>
<td><p>additional params to pass</p></td>
</tr>
</tbody>
</table>

### Details

Prepared SQL with properly quoted parameters are provided through the
`statement` param, where the FROM fields should use the token :data:.
This token is then replaced with the rendered SQL chain from the object
being queried, effectively appending the new instructions. This set of
appended instructions are then used to update the object.


---
## StreamSpatialToDB arrow

### Description

Stream spatial data to database (arrow)

### Usage

    streamSpatialToDB_arrow(
      path,
      backend_ID,
      remote_name = "test",
      id_col = "poly_ID",
      xy_col = c("x", "y"),
      extent = NULL,
      file_format = NULL,
      chunk_size = 10000L,
      callback = NULL,
      overwrite = FALSE,
      custom_table_fields = NULL,
      custom_table_fields_attr = NULL,
      ...
    )

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="streamSpatialToDB_arrow_:_path">path</code></td>
<td><p>path to database</p></td>
</tr>
<tr class="even">
<td><code
id="streamSpatialToDB_arrow_:_backend_ID">backend_ID</code></td>
<td><p>backend ID of database</p></td>
</tr>
<tr class="odd">
<td><code
id="streamSpatialToDB_arrow_:_remote_name">remote_name</code></td>
<td><p>name of table to generate in database backend</p></td>
</tr>
<tr class="even">
<td><code id="streamSpatialToDB_arrow_:_id_col">id_col</code></td>
<td><p>ID column of data</p></td>
</tr>
<tr class="odd">
<td><code id="streamSpatialToDB_arrow_:_xy_col">xy_col</code></td>
<td><p>character vector. Names of x and y value spatial coordinates or
vertices</p></td>
</tr>
<tr class="even">
<td><code id="streamSpatialToDB_arrow_:_extent">extent</code></td>
<td><p>terra SpatExtent (optional) that can be used to subset the data
to read in before it is saved to database</p></td>
</tr>
<tr class="odd">
<td><code
id="streamSpatialToDB_arrow_:_file_format">file_format</code></td>
<td><p>(optional) file type (one of csv/tsv, arrow, or parquet)</p></td>
</tr>
<tr class="even">
<td><code
id="streamSpatialToDB_arrow_:_chunk_size">chunk_size</code></td>
<td><p>chunk size to use when reading in data</p></td>
</tr>
<tr class="odd">
<td><code id="streamSpatialToDB_arrow_:_callback">callback</code></td>
<td><p>callback function to allow access to read-chunk-level data
formatting and filtering. Input and output should both be
data.table</p></td>
</tr>
<tr class="even">
<td><code id="streamSpatialToDB_arrow_:_overwrite">overwrite</code></td>
<td><p>logical. whether to overwrite if table already exists in database
backend</p></td>
</tr>
<tr class="odd">
<td><code
id="streamSpatialToDB_arrow_:_custom_table_fields">custom_table_fields</code></td>
<td><p>default = NULL. If desired, custom setup the fields of the table.
See <code>dbCreateTable</code></p></td>
</tr>
<tr class="even">
<td><code
id="streamSpatialToDB_arrow_:_custom_table_fields_attr">custom_table_fields_attr</code></td>
<td><p>default = NULL. If desired, custom setup the fields of the paired
attributes table</p></td>
</tr>
</tbody>
</table>

### Value

invisibly returns the final geom ID used


---
## StreamToDB fread

### Description

Files are read in chunks of lines via `fread` and then converted to the
required formatting with plugin functions provided through the
`callback` param before being written/appended to the database table.
This is slower than directly writing the information in, but is a
scalable approach as it never requires the full dataset to be in
memory.  
If more than one or a custom callback is needed for the formatting then
a combined or new function can be defined on the spot as long as it
accepts `data.table` input and returns a `data.table`.

### Usage

    streamToDB_fread(
      path,
      backend_ID,
      remote_name = "test",
      idx_col = NULL,
      pk = NULL,
      nlines = 10000L,
      cores = 1L,
      callback = NULL,
      overwrite = FALSE,
      custom_table_fields = NULL,
      ...
    )

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="streamToDB_fread_:_path">path</code></td>
<td><p>path to the matrix file</p></td>
</tr>
<tr class="even">
<td><code id="streamToDB_fread_:_backend_ID">backend_ID</code></td>
<td><p>ID of the backend to use</p></td>
</tr>
<tr class="odd">
<td><code id="streamToDB_fread_:_remote_name">remote_name</code></td>
<td><p>name to assign table of read in values in database</p></td>
</tr>
<tr class="even">
<td><code id="streamToDB_fread_:_idx_col">idx_col</code></td>
<td><p>character. If not NULL, the specified column will be generated as
unique ascending integers</p></td>
</tr>
<tr class="odd">
<td><code id="streamToDB_fread_:_pk">pk</code></td>
<td><p>character. Which columns to use as primary key</p></td>
</tr>
<tr class="even">
<td><code id="streamToDB_fread_:_nlines">nlines</code></td>
<td><p>integer. Number of lines to read per chunk</p></td>
</tr>
<tr class="odd">
<td><code id="streamToDB_fread_:_cores">cores</code></td>
<td><p>fread cores to use</p></td>
</tr>
<tr class="even">
<td><code id="streamToDB_fread_:_callback">callback</code></td>
<td><p>callback functions to apply to each data chunk before it is sent
to the database backend</p></td>
</tr>
<tr class="odd">
<td><code id="streamToDB_fread_:_overwrite">overwrite</code></td>
<td><p>whether to overwrite if table already exists (default =
FALSE)</p></td>
</tr>
<tr class="even">
<td><code
id="streamToDB_fread_:_custom_table_fields">custom_table_fields</code></td>
<td><p>default = NULL. If desired, custom setup the fields of the table.
See <code>dbCreateTable</code></p></td>
</tr>
<tr class="odd">
<td><code id="streamToDB_fread_:_...">...</code></td>
<td><p>additional params to pass to fread</p></td>
</tr>
</tbody>
</table>


---
## Svpoint to dt

### Description

SpatVector point to data.table

### Usage

    svpoint_to_dt(spatvector, include_values = TRUE)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="svpoint_to_dt_:_spatvector">spatvector</code></td>
<td><p>spatvector to use</p></td>
</tr>
<tr class="even">
<td><code id="svpoint_to_dt_:_include_values">include_values</code></td>
<td><p>whether to include attribute values in output</p></td>
</tr>
</tbody>
</table>


---
## TableBE

### Description

Create a table from database

### Usage

    tableBE(cPool, remote_name, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="tableBE_:_cPool">cPool</code></td>
<td><p>database connection pool object</p></td>
</tr>
<tr class="even">
<td><code id="tableBE_:_remote_name">remote_name</code></td>
<td><p>name of table with database</p></td>
</tr>
<tr class="odd">
<td><code id="tableBE_:_...">...</code></td>
<td><p>additional params to pass to dplyr::tbl()</p></td>
</tr>
</tbody>
</table>


---
## TableInfo

### Description

Get information about the database table

### Usage

    ## S4 method for signature 'ANY,character'
    tableInfo(x, remote_name, ...)

    ## S4 method for signature 'dbData,missing'
    tableInfo(x, remote_name, ...)

    ## S4 method for signature 'dbPolygonProxy,missing'
    tableInfo(x, remote_name, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="tableInfo_:_x">x</code></td>
<td><p>connector object (hashID of backend, DBI connection, pool), or a
duckling object</p></td>
</tr>
<tr class="even">
<td><code id="tableInfo_:_remote_name">remote_name</code></td>
<td><p>(only needed if x is a connection object) name of table on
DB</p></td>
</tr>
</tbody>
</table>

### Value

a data.table of information about the specified table on the database

### Examples

```r
dbpoly = simulate_dbPolygonProxy()
tableInfo(dbpoly)

dbpoints = simulate_dbPointsProxy()
tableInfo(dbpoints)

dbDF = simulate_dbDataFrame()
tableInfo(dbDF)
```


---
## ValidBE-generic

### Description

Check if DB backend object has valid connection

### Usage

    ## S4 method for signature 'dbData'
    validBE(x)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="validBE-generic_:_x">x</code></td>
<td><p>Duckling object</p></td>
</tr>
</tbody>
</table>


---
## WriteRemoteTable

### Description

Write a persistent table to the database

### Usage

    writeTableBE(cPool, remote_name, value, ...)

### Arguments

<table>
<tbody>
<tr class="odd">
<td><code id="writeRemoteTable_:_cPool">cPool</code></td>
<td><p>database connection pool object</p></td>
</tr>
<tr class="even">
<td><code id="writeRemoteTable_:_remote_name">remote_name</code></td>
<td><p>name to assign the table within the database</p></td>
</tr>
<tr class="odd">
<td><code id="writeRemoteTable_:_value">value</code></td>
<td><p>table to write</p></td>
</tr>
<tr class="even">
<td><code id="writeRemoteTable_:_...">...</code></td>
<td><p>additional params to pass to DBI::dbWriteTable()</p></td>
</tr>
</tbody>
</table>


---
