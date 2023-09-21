#' @describeIn simulate_objects Simulate a duckdb connection dplyr tbl_Pool in memory
#' @keywords internal
sim_duckdb = function(data = datasets::iris, name = 'test', p = NULL) {
  # setup in-memory db if no pool connection provided
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
#' @title sim_dgc
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
#' sim_dgc()
#'
#' @keywords internal
sim_matrix = function(mat_type = c('dense', 'sparse'), 
                      num_rows = 50, 
                      num_cols = 50, 
                      seed_num = 42){
  # check if num_rows and num_cols are both greater than or equal to 10
  if (num_rows < 10 | num_cols < 10) {
    stop("Number of rows and columns must be at least 10.")
  }
  
  # check if mat_type is either dense or sparse
  if (mat_type != 'dense' & mat_type != 'sparse') {
    stop("mat_type must be either 'dense' or 'sparse'.")
  }
  
  # for reproducibility
  set.seed(seed_num)

  # setup dummy matrix data
  if(mat_type == 'sparse'){

    # create simulated dgc matrix
    data <- matrix(0, nrow = num_rows, ncol = num_cols)

    # Set n random values to non-zero
    non_zero_indices <- sample(1:(50*50), 50)
    data[non_zero_indices] <- rnorm(50)

    # Create dumby sparse dgc matrix (column-major) # TODO: row-major sparse matrices
    mat = as(data, "dgCMatrix")
    
    # Create sparse ijx representation
    ijx = Matrix::summary(mat)
    
  } else { # dense matrix
    # Create dumby dataset
    mat = matrix(rnorm(num_rows * num_cols), nrow = num_rows, ncol = num_cols)
    
    # hardcode dimnames for simulated data
    row_names = as.factor(paste0("row", 1:num_rows))
    col_names = as.factor(paste0("col", 1:num_cols))
    
    # set dimnames for matrix
    rownames(mat) = row_names
    colnames(mat) = col_names
    
    # Create dense ijx representation
    ijx <- dplyr::tibble(
      i = rownames(mat)[row(mat)],
      j = colnames(mat)[col(mat)],
      x = as.numeric(mat)
    )
    
  }
  
  return(ijx)
}

#' @describeIn simulate_objects Simulate a dbDataFrame in memory
#' @export
sim_dbDataFrame = function(data = NULL, name = 'df_test', key = NA_character_) {
  if(is.null(data)) {
    data = sim_duckdb(name = name)
  }
  if(!inherits(data, 'tbl_sql')) {
    checkmate::assert_class(data, 'data.frame')
    data = sim_duckdb(data = data, name = name)
  }
  dbDataFrame(data = data, remote_name = name, hash = 'ID_dummy',
              init = TRUE, key = key)
}

#' @describeIn simulate_objects
#' @details Simulate a dbSparseMatrix in memory
#' @export
sim_dbSparseMatrix = function(dgc = NULL, name = 'sparse_test') {
  if(is.null(dgc)) {
    dgc = sim_matrix(mat_type = 'sparse',
                     num_rows = 50,
                     num_cols = 50,
                     seed_num = 42)

    data = sim_duckdb(data = dgc, name = name)
  }
  if(!inherits(data, 'tbl_sql')) {
    if(!inherits(dgc, 'dgCMatrix')) {
      stop("Only dgCMatrix is supported for now.")
    }
    # create sparse ijx representation of dgc
    ijx = Matrix::summary(dgc)
    
    # add ijx to db
    data = sim_duckdb(data = ijx, name = name)
  }
  
  # hardcode dimnames for simulated data
  row_names = as.factor(paste0("row", 1:num_rows))
  col_names = as.factor(paste0("col", 1:num_cols))
  
  # create dbSparseMatrix obj
  res = dbSparseMatrix(data = data, remote_name = name, hash = 'ID_dummy',
                dims = c(50L,50L), dim_names = list(row_names, col_names),
                init = TRUE)
  
  return(res)
}

#' @describeIn simulate_objects Simulate a dbSparseMatrix in memory
#' @export
sim_dbDenseMatrix = function(data = NULL, name = 'dense_test') {
  if(is.null(data)) {
    data = sim_matrix(mat_type = 'dense',
                     num_rows = 50,
                     num_cols = 50,
                     seed_num = 42)
    data = sim_duckdb(data = data, name = name)
  }
  if(!inherits(data, 'tbl_sql')) {
    checkmate::assert_class(data, 'matrix')
    
    # require rownames and colnames in matrices for now
    # TODO: accept matrices without rownames and colnames
    if(is.null(rownames(data)) | is.null(colnames(data))) {
      stop("Input matrix with rownames and colnames is only currrently supported.
           Please provide an input matrix with rownames and colnames.")
    }
    
    # create sparse ijx representation of dgc
    ijx <- dplyr::tibble(
      i = rownames(data)[row(data)],
      j = colnames(data)[col(data)],
      x = as.numeric(data)
    )
    
    # add ijx to db
    data = sim_duckdb(data = ijx, name = name)
  }

  # hardcode dimnames for simulated data
  row_names = as.factor(paste0("row", 1:num_rows))
  col_names = as.factor(paste0("col", 1:num_cols))

  # create obj
  res = dbDenseMatrix(data = data, remote_name = name, hash = 'ID_dummy',
                dims = c(50L,50L), dim_names = list(row_names, col_names),
                init = TRUE)
  
  return(res)
}

#' @describeIn simulate_objects Simulate a dbPointsProxy in memory
#' @export
sim_dbPointsProxy = function(data = NULL) {
  if(is.null(data)) {
    gpoint = GiottoData::loadSubObjectMini('giottoPoints')
    sv_dt = svpoint_to_dt(gpoint[], include_values = TRUE)
    data = sim_duckdb(data = sv_dt, name = 'pnt_test')
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

    data = sim_duckdb(data = sv_geom, name = 'poly_test')
    data_atts = sim_dbDataFrame(
      sim_duckdb(sv_atts, p = cPool(data), name = 'poly_test_attr'),
      key = 'geom',
      name = 'poly_test_attr'
    )
  }
  dbPolygonProxy(data = data, remote_name = 'poly_test', hash = 'ID_dummy',
                 n_poly = nrow(sv_atts), init = TRUE, extent = terra::ext(gpoly[]),
                 attributes = data_atts)
}







