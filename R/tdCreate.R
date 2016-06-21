# TODO: Function to create table in Teradata
#
# Author: Linh Tran
# Date: Jun 18, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdCreate
#'
#' @description Creates a table in Teradata and (optionally) uploads data. Can take a JDBC
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
#' If not provided, the columns will be defined according to the class of each column in
#' the provided \code{data.frame}.
#' \itemize{
#'		\item{numeric}{Integer if maximum value is under 2,000,000. Otherwise, bigint.}
#'		\item{date}{Date}
#'		\item{POSIXct or POSIXt}{Timestamp(0)}
#'		\item{All others}{varchar with length equal to the longest character length}
#' '}
#'
#' Please note that in order to keep the required Java memory down, data is uploaded in batches
#' at a time. This can be configured to be as many (or little observations) as desired.
#'
#' @param data \code{link{data.frame}} containing data to upload. This must be in
#' the same column order as the Teradata table.
#' @param table Name of the Teradata table to upload data to. If \code{NULL}, then the name of the
#' \code{data.frame} provided will be used as the Teradata table name, with periods replaced by
#' underscores.
#' @param upload logical. If \code{TRUE}, then the function will call \code{tdUpload} to
#' upload the data after creating the table.
#' @param colType Vector of column types for Teradata table. If NULL, will be inferred following
#' approach listed in details section.
#' @param pi Primary index for Teradata table. If NULL, will take the first column with all unique
#' values as the primary index. If none exists, will take the first column in \code{data}.
#' @param batchSize Number of rows to upload simultaneously. Only used if \code{upload=TRUE}.
#' @param verbose logical. If \code{TRUE}, then print message after each batch is uploaded.
#' @param ... Optional connection settings.
#'
#' @return Returns an invisible objecting containing the count of the number of rows uploaded.
#'
#' @seealso
#' \code{\link{tdConn}} for connection, \code{\link{tdUpload}} for table upload,
#' \code{\link{td}} for general queries, \code{\link{tdCpu}} for CPU
#' usage.
#'
#'
#' @export
tdCreate = function(data=NULL, table=NULL, upload=TRUE, colType=NULL, pi=NULL, batchSize=2500, verbose=TRUE, ...) {

	## Column type ##
	supportedTypes = c("numeric", "Date", "POSIXct", "POSIXt", "character")
	tmp = unlist(lapply(sapply(data, class), function(x) return(x[1])))
	if (any(!tmp %in% supportedTypes)) stop("Unsupported data type included.")
	if (is.null(colType)) {
		colType = sapply(data, function(x) {
				tmpClass = class(x)
				if (tmpClass=="numeric") {
					if (max(x) < 2000000) {
						foo = "integer"
					} else foo = "bigint"
				} else if (tmpClass=="Date") {
					foo = "date"
				} else if (tmpClass %in% c("POSIXct", "POSIXt")) {
					foo = "timestamp(0)"
				} else {
					stringLength = max(nchar(as.character(x)))
					foo = paste0("varchar(", stringLength, ")")
				}
				return(foo)
			})
	} else {
		if (length(colType)!=ncol(data)) stop("length of colType must match number of columns.")
	}
	colType = gsub("\\.", "_", colType)

	## Table name ##
	if(is.null(table)) {
		tmp = try(eval(data), TRUE)
		if (!inherits(tmp, "try-error")) table = paste(substitute(list(data)))[-1]
		if (is.null(table) | all(table=='')) stop("No Teradata table specified.")
		table = gsub("\\.", "_", table)
	} else {
		table = strsplit(table, "\\.")[[1]]
		if (length(table)>2) stop("Table names can only have up to 1 period.")
	}

	## Primary index ##
	if (!is.null(pi)) {
		if (upper(pi) %in% upper(names(data))) stop(paste(pi, "was not found in the column names."))
	} else {
		piCounts = sapply(data, function(x) {
				return(length(unique(x)))
			})
		pi = names(piCounts[piCounts==nrow(data)][1])
		if (is.na(pi)) pi = names(data)[1]
	}

	## Connection ##
	conn = tdCheckConn(list(...))
	tmpConnection = attr(conn, "tmpConnection")
	attr(conn, "tmpConnection") = FALSE

	## Create table ##
	if (verbose) cat("Creating table...")
	cols = paste(gsub("\\.", "_", names(data)), colType)
	td(sprintf(
		"create multiset table %s,
		NO FALLBACK,
	    NO BEFORE JOURNAL,
	    NO AFTER JOURNAL,
	    CHECKSUM = DEFAULT,
	    DEFAULT MERGEBLOCKRATIO
	    (
			%s
		)
		primary index (%s);", table, paste(cols, collapse="\n, "), pi), conn=conn)
	if (verbose) cat("\n")

	## Upload data ##
	uploadResult = 0
	if (upload) {
		uploadResult = tdUpload(data=data, table=table, batchSize=batchSize, verbose=verbose, conn=conn)
	}

	## Connection ##
	if (tmpConnection) DBI::dbDisconnect(conn)

	invisible(uploadResult)

}

