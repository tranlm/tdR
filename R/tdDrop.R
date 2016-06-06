# TODO: Function to delete tables from Teradata
#
# Author: Linh Tran
# Date: Mar 10, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdDrop
#'
#' @description Drops tables from Teradata. Can take a JDBC connection
#' object (\code{conn}) if provided. If no JDBC connection is provided, then
#' a connection is attempted using the \code{user}, and \code{password} provided.
#' If none is provided, then tries to locate a connection object (\code{conn})
#' in the global environment.
#'
#' If a connection profile (e.g. username, password, etc.) is provided, then
#' an attempt is made to connect to Teradata. Once the query is run, the
#' connection is then closed. If a connection object (\code{conn}) is provided
#' to the function (or one is found globally), then the connection remains
#' open.
#'
#' @param tables String vector of Teradata tables to drop
#' @param ... Optional connection settings.
#'
#' @return An \code{\link{invisible}} object containing the tables dropped 
#' is returned.
#'
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdDisk}} for disk usage,
#' \code{\link{tdSpool}} for spool usage, \code{\link{tdCpu}} for CPU 
#' usage, and \code{\link{tdJoin}} for joining tables.
#' 
#' @examples
#' ## NOT RUN ##
#' ## Runs a quick drop query based on connection profile
#' # tdDrop(<tableName>, username=<username>, password=<password>, db="GCA")
#'
#' ## Runs query using a separately established connection
#' # conn = tdConn(<username>, <password>, db="GCA")
#' # tdDrop(<tableName>, conn=conn)
#'
#' ## Uses same connection, but allows code to find globally. 
#' # Can also drop multiple tables. 
#' # tdDrop(c(<table1Name>, <table2Name>))
#'
#' @export
tdDrop = function(tables="", ...) {
	
	if (is.null(tables) | all(tables=='')) stop("No Teradata table specified.")
	tables = strsplit(toupper(tables), "\\.")
	if (any(unlist(lapply(tables, length))>2)) stop("Table names can only have up to 1 period.")
	
	## Connection ##
	conn = tdCheckConn(list(...))
	
	## Tables ##
	tables = lapply(tables, function(x) {
		if (length(x) %in% c(1,2)) {
			return(paste(x, collapse="."))
		} else {
			stop("Problem with the table names.")
		}
	})
	
	## Tables ##
	removed = NULL
	for(i in 1:length(tables)) {
		res = try(td(sprintf('drop table %s;', tables[[i]]), conn=conn), TRUE)
		if (!inherits(res, "try-error")) removed = c(removed, tables[[i]])
		rm(res)
	}

	## Connection ##
	if (attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)
	
	## Output ##
	invisible(removed)
	
}
