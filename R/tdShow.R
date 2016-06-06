# TODO: Function to show the Teradata table
# 
# Author: Linh Tran
# Date: Mar 11, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdShow
#'
#' @description Gets the Teradata code used to create the table. Can take a JDBC 
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
#' Many of the core production tables in Teradata are locked, such that 
#' trying to query to show table code will result in \code{Error 3853}. 
#' This is a part of the user restrictions and can be circumvented by creating 
#' a new subset of the table and querying that subset. If the Teradata code 
#' is unable to be determined, a value of \code{NA} will be returned.
#' 
#' @param table String of name of table to get Teradata code from.
#' @param ... Optional connection settings.
#'
#' @return Returns a string \code{\link{vector}} of the the Teradata code. 
#' 
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdDisk}} for disk usage,
#' \code{\link{tdSpool}} for spool usage, \code{\link{tdCpu}} for CPU 
#' usage, and \code{\link{tdJoin}} for joining tables.
#' 
#' @examples
#' ## NOT RUN (will also result in errors due to user restrictions) ##
#' ## Runs a quick query based on connection profile
#' # tdShow("ICDB_PERSON", username=<username>, password=<password>, db="GCA")
#'
#' ## Runs query using a separately established connection
#' # conn = tdConn(<username>, <password>, db="GCA")
#' # tdShow("ICDB_PERSON", conn=conn)
#'
#' ## Uses same connection, but allows code to find globally. Also used for multiple tables.
#' # tdShow(c("ICDB_PERSON", "ICDB_PERSON_X"))
#'
#' @export
tdShow = function(table=NULL, ...) {
	
	tmp = try(eval(table), TRUE)
	if (inherits(tmp, "try-error")) tmp = paste(substitute(list(table)))[-1]
	if (!exists(tmp)) table=tmp
	if (is.null(table) | all(table=='')) stop("No Teradata table specified.")
	table = strsplit(toupper(table), "\\.")[[1]]
	if (length(table)>2) stop("table name can only have up to 1 period.")
	
	## Connection ##
	conn = tdCheckConn(list(...))

	## Query ##
	showResults = ''
	tmpTry = try(td(sprintf('show table %s;', paste(table, collapse=".")), conn=conn)[1,1], TRUE)
	if (inherits(tmpTry, 'try-error')) {
		tmpTry = try(td(sprintf('show view from %s;', paste(table, collapse=".")), conn=conn)[1,1], TRUE)
	}
	if (inherits(tmpTry, 'try-error')) {
		tmpTry = try(td(sprintf('show select * from %s;', paste(table, collapse=".")), conn=conn)[1,1], TRUE)
	}
	if (!inherits(tmpTry, 'try-error')) {
		showResults[1] = tmpTry
	}
	showResults = paste(gsub("\\r", "\\\n", showResults), "\n")

	## Connection ##
	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)
	
	class(showResults) = "tdShow"
	return(showResults)
}

#' @export
print.tdShow = function(x, ...) {
	cat(x)
	invisible(x)
}