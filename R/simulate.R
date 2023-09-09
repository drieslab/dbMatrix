#' @name simulate_objects
#' @title Simulate Duckling objects
#' @description
#' Create simulated Duckling objects in memory from pre-prepared data. Useful
#' for testing purposes and examples.
#' @param data data to use
#' @param name remote name of table on database to set
NULL

# create_sparse_ijx_dt
#
# create ijx vector representation of sparse matrix, keeping zeros
#
# Updates dgcmatrix by reference
#
#' @noRd
create_dense_ijx_dt <- function(dgc){
  dplyr::tibble(
    i=rownames(dgc)[row(dgc)],
    j=colnames(dgc)[col(dgc)],
    x=as.numeric(dgc)
  )
}


#' @describeIn simulate_objects Simulate a duckdb connection dplyr tbl_Pool in memory
#' @internal
simulate_duckdb = function(data = datasets::iris, name = 'test', p = NULL) {
  if(is.null(p)) {
    drv = duckdb::duckdb(dbdir = ':memory:')
    p = pool::dbPool(drv)
  }
  conn = pool::poolCheckout(p)
  duckdb::duckdb_register(conn, df = data, name = name)
  pool::poolReturn(conn)
  dplyr::tbl(p, name)
}

#' @describeIn simulate_objects Simulate a duckdb connection dplyr tbl_Pool in memory
#' @title simulate_dgc
#' @description Simulate a dgCMatrix object
#'
#' @details
#' This function generates a dgCMatrix object with a specified number of rows and columns,
#' and sets 50 random values to non-zero.
#'
#' @param num_rows The number of rows in the matrix (default: 50)
#' @param num_cols The number of columns in the matrix (default: 50)
#' @param seed_num The seed number for reproducibility (default: 42)
#'
#' @return A dgCMatrix object
#'
#' @examples
#' simulate_dgc()
#'
#' @internal
simulate_dgc = function(num_rows = 50, num_cols = 50, seed_num = 42){
    # check if num_rows and num_cols are both greater than or equal to 10
  if (num_rows < 10 | num_cols < 10) {
    stop("Number of rows and columns must be at least 10.")
  }

  # setup dummy matrix data
  data <- matrix(0, nrow = num_rows, ncol = num_cols)

  # Set n random values to non-zero
  set.seed(seed_num) # for reproducibility
  non_zero_indices <- sample(1:(50*50), 50)
  data[non_zero_indices] <- rnorm(50)

  # Create dgc matrix
  dgc = as(data, "dgCMatrix")

  # hardcode dimnames
  row_names = as.factor(paste0("row", 1:num_rows))
  col_names = as.factor(paste0("col", 1:num_cols))

  # set dimnames
  rownames(dgc) = row_names
  colnames(dgc) = col_names

  return(dgc)
}

#' @describeIn simulate_objects Simulate a dbDataFrame in memory
#' @export
sim_dbDataFrame = function(data = NULL, name = 'df_test', key = NA_character_) {
  if(is.null(data)) {
    data = simulate_duckdb(name = name)
  }
  if(!inherits(data, 'tbl_sql')) {
    checkmate::assert_class('data.frame')
    data = simulate_duckdb(data = data, name = name)
  }
  dbDataFrame(data = data, remote_name = name, hash = 'ID_dummy',
              init = TRUE, key = key)
}

#' @describeIn simulate_objects Simulate a dbSparseMatrix in memory
#' @export
sim_dbSparseMatrix = function(dgc = NULL, name = 'sparse_test') {
  if(is.null(dgc)) {
    data = create_dbSparseMatrix(name = name)
  }
  if(!inherits(data, 'tbl_sql')) {
    checkmate::assert_class(data, 'dbSparseMatrix')
    data = create_dbSparseMatrix(data = dgc, name = name)
  }

  dbSparseMatrix(data = data, remote_name = name, hash = 'ID_dummy',
                 dims = c(50L,50L), init = TRUE)
}

#' @describeIn simulate_objects Simulate a dbSparseMatrix in memory
#' @export
sim_dbDenseMatrix = function(dgc = NULL, name = 'dense_test') {
  if(is.null(dgc)) {
    data = create_dbDenseMatrix(name = name)
  }
  if(!inherits(data, 'tbl_sql')) {
    checkmate::assert_class(data, 'dbDenseMatrix')
    data = create_dbDenseMatrix(data = dgc, name = name)
  }

  # row_names <- as.factor(paste0("row", 1:50)) # hardcode for now
  # col_names <- as.factor(paste0("col", 1:50)) # hardcode for now
  dbDenseMatrix(data = data, remote_name = name, hash = 'ID_dummy',
                 dims = c(50L,50L), init = TRUE)
}

#' @describeIn simulate_objects Simulate a dbPointsProxy in memory
#' @export
sim_dbPointsProxy = function(data = NULL) {
  if(is.null(data)) {
    gpoint = GiottoData::loadSubObjectMini('giottoPoints')
    sv_dt = svpoint_to_dt(gpoint[], include_values = TRUE)
    data = simulate_duckdb(data = sv_dt, name = 'pnt_test')
  }
  dbPointsProxy(data = data, remote_name = 'pnt_test', hash = 'ID_dummy',
                n_point = nrow(sv_dt), init = TRUE, extent = terra::ext(gpoint[]))
}


#' @describeIn simulate_objects Simulate a dbPolygonProxy in memory
#' @export
sim_dbPolygonProxy = function(data = NULL) {
  if(is.null(data)) {
    gpoly = GiottoData::loadSubObjectMini('giottoPolygon')
    sv_geom = data.table::setDT(terra::geom(gpoly[], df = TRUE))
    sv_atts = data.table::setDT(terra::values(gpoly[]))
    sv_atts[, geom := seq(.N)]
    data.table::setcolorder(sv_atts, neworder = c('geom', 'poly_ID'))

    data = simulate_duckdb(data = sv_geom, name = 'poly_test')
    data_atts = simulate_dbDataFrame(
      simulate_duckdb(sv_atts, p = cPool(data), name = 'poly_test_attr'),
      key = 'geom',
      name = 'poly_test_attr'
    )
  }
  dbPolygonProxy(data = data, remote_name = 'poly_test', hash = 'ID_dummy',
                 n_poly = nrow(sv_atts), init = TRUE, extent = terra::ext(gpoly[]),
                 attributes = data_atts)
}







