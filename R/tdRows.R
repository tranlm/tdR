# TODO: Function to grab Teradata table rows
# 
# Author: Linh Tran
# Date: Mar 11, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdRows
#'
#' @description Gets the rows in Teradata tables. Can take a JDBC 
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
#' @param table String vector of name of table to get table dimensions from.
#' @param where String statement to subset table with. 
#' @param ... Optional connection settings.
#'
#' @return Returns a the number of Teradata table rows. 
#' 
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdNames}} for table names,
#' \code{\link{td}} for general queries, \code{\link{tdCpu}} for CPU 
#' usage, and \code{\link{tdJoin}} for joining tables.
#' 
#' @examples
#' ## NOT RUN (will also result in errors due to user restrictions) ##
#' ## Runs a quick query based on connection profile
#' # tdRows("ICDB_PERSON", username=<username>, password=<password>, db="GCA")
#'
#' ## Runs query using a separately established connection
#' # conn = tdConn(<username>, <password>, db="GCA")
#' # tdRows("ICDB_PERSON", conn=conn)
#'
#' ## Uses same connection, but allows code to find globally. 
#' # tdRows("ICDB_PERSON")
#'
#' @export
tdRows = function(table=NULL, where="", ...) {
	
	tmp = try(eval(table), TRUE)
	if (inherits(tmp, "try-error")) tmp = paste(substitute(list(table)))[-1]
	if (!exists(tmp)) table=tmp
	if (is.null(table) | all(table=='')) stop("No Teradata table specified.")
	table = strsplit(toupper(table), "\\.")[[1]]
	if (length(table)>2) stop("Table names can only have up to 1 period.")
	
	## Connection ##
	conn = tdCheckConn(list(...))
	
	## Subset ##
	if (where!="") where = paste("where", where)
	
	tmpRows = td(sprintf("select cast(count(*) as bigint) from %s %s;", paste(table, collapse="."), where), conn=conn)[1,1]
	
	## Connection ##
	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)
	
	return(tmpRows)
}

