# TODO: Function to grab Teradata table column count
# 
# Author: Linh Tran
# Date: Mar 11, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdDim
#'
#' @description Gets the columns in Teradata tables. Can take a JDBC 
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
#' @param table String vector of name of table to get table columns from.
#' @param ... Optional connection settings.
#'
#' @return Returns the number of Teradata table columns. 
#' 
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdNames}} for table names,
#' \code{\link{td}} for general queries, \code{\link{tdCpu}} for CPU 
#' usage, and \code{\link{tdJoin}} for joining tables.
#' 
#' @examples
#' ## NOT RUN (will also result in errors due to user restrictions) ##
#' ## Runs a quick query based on connection profile
#' # tdCols("ICDB_PERSON", username=<username>, password=<password>, db="GCA")
#'
#' ## Runs query using a separately established connection
#' # conn = tdConn(<username>, <password>, db="GCA")
#' # tdCols("ICDB_PERSON", conn=conn)
#'
#' ## Uses same connection, but allows code to find globally. 
#' # tdCols("ICDB_PERSON")
#'
#' @export
tdCols = function(table=NULL, ...) {
	
	tmp = try(eval(table), TRUE)
	if (inherits(tmp, "try-error")) tmp = paste(substitute(list(table)))[-1]
	if (!exists(tmp)) table=tmp
	if (is.null(table) | all(table=='')) stop("No Teradata table specified.")
	table = strsplit(toupper(table), "\\.")[[1]]
	if (length(table)>2) stop("Table names can only have up to 1 period.")
	
	## Connection ##
	conn = tdCheckConn(list(...))
	
	## Subset ##
	qry = paste("where", ifelse(length(table)==1, sprintf("upper(TableName)='%s'", table[1]), ifelse(length(table)==2, sprintf("upper(DatabaseName)='%s' AND upper(TableName)='%s'", table[1], table[2]), "")))
	tableDim = td(sprintf("SELECT count(columnname) as ncol FROM DBC.Columns %s;", qry), conn=conn)$ncol
	
	## Connection ##
	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)
	
	return(tableDim)
}

