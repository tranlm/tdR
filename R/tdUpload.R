# TODO: Function to upload data to Teradata
#
# Author: Linh Tran
# Date: Mar 11, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdUpload
#'
#' @description Uploads data to Teradata tables. Can take a JDBC
#' connection object (\code{conn}) if provided. If no JDBC connection is provided,
#' then a connection is attempted using the \code{user}, and \code{password}
#' provided. If none is provided, then tries to locate a connection object
#' (\code{conn}) in the global environment.
#'
#' If a connection profile (e.g. username, password, etc.) is provided, then
#' an attempt is made to connect to Teradata. Once the query is run, the
#' connection is then closed. If a connection object (\code{conn}) is provided
#' to the function (or one is found globally), then the connection remains
#' open.
#'
#' @details
#' By default, numeric vectors will be uploaded as an int, while character vectors
#' will be uploaded as a varchar with the longest character length. For now, if any
#' other \code{class} is included, an error will be thrown.
#'
#' In order to keep the required Java memory down, data is uploaded in batches
#' at a time. This can be configured to be as many (or little observations) as desired.
#' If any of the batched observations have bad values, then all observations corresponding
#' with that batch will fail to load. It might help in this situation to reduce the batch size
#' to pinpoint which observation(s) is(are) causing the issue.
#'
#' @param data \code{link{data.frame}} containing data to upload. This must be in
#' the same column order as the Teradata table.
#' @param table Name of the Teradata table to upload data to.
#' @param batchSize Number of rows to upload simultaneously.
#' @param verbose logical. If \code{TRUE}, then print progress after each batch is uploaded.
#' @param checkTable logical. If \code{TRUE}, then assumed that table already exists. If \code{FALSE},
#' then check whether table exists. Used internally with \code{tdCreate} function to cut down on processing time.
#' @param ... Optional connection settings.
#'
#' @return Returns an invisible objecting containing the count of the number of rows uploaded.
#'
#' @seealso
#' \code{\link{tdConn}} for connection, \code{\link{tdNames}} for table names,
#' \code{\link{td}} for general queries, \code{\link{tdCpu}} for CPU
#' usage, and \code{\link{tdJoin}} for joining tables.
#'
#'
#' @export
tdUpload = function(data=NULL, table=NULL, batchSize=2500, verbose=TRUE, checkTable=TRUE, ...) {

	supportedTypes = c("numeric", "character", "Date", "POSIXct", "POSIXt")
	tmp = unlist(lapply(sapply(data, class), function(x) return(x[1])))
	table = strsplit(toupper(table), "\\.")[[1]]
	if (length(table)>2) stop("Table names can only have up to 1 period.")

	if (any(!tmp %in% supportedTypes)) stop("Unsupported data type included.")

	## Connection ##
	conn = tdCheckConn(list(...))

	## Query ##
	table = paste(table, collapse=".")
	if (checkTable) {
		if (!tdExists(table)) stop("Table not found.")	
	}
	if (verbose) pb = txtProgressBar(max=nrow(data), style=3)
	
	ps = .jcall(conn@jc, "Ljava/sql/PreparedStatement;", "prepareStatement", sprintf("insert into %s values(%s)", table, paste(rep("?", ncol(data)), collapse=",")))
	rowsUploaded = tmpi = 0
	badObs = NULL
	for(i in 1:nrow(data)) {
		for(j in 1:ncol(data)) {
			if (class(data[[j]])[1]=="numeric") {
				.jcall(ps,"V", "setDouble", as.integer(j), data[i,j])
			} else {
				.jcall(ps,"V", "setString", as.integer(j), as.character(data[i,j]))
			}
		}
		.jcall(ps,"V", "addBatch")
		if (i %% batchSize == 0 | i==nrow(data)) {
			loadResult = try(.jcall(ps,"[I","executeBatch"), TRUE)
			if (inherits(loadResult, "try-error")) {
				badObs = c(badObs, (tmpi+1):i)
			} else {
				rowsUploaded = rowsUploaded + sum(loadResult)
			}
			if(verbose) {
				setTxtProgressBar(pb, i)
			}
			tmpi = i
		}
	}
	.jcall(ps,"V","close")
	if (verbose) cat("\n")
	if (length(badObs)>0) warning(paste("Error with data. Did not upload the following rows: ", paste(badObs, collapse=", ")))

	## Connection ##
	if (attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)

	invisible(rowsUploaded)

}

