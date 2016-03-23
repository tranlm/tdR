# TODO: Function to look for whether table exists.
# 
# Author: Linh Tran
# Date: Dec 12, 2015
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdExists
#'
#' @description Determines if Teradata table exists. This code is specifically 
#' designed for connectivity to Teradata servers using OSX at Apple using JDBC 
#' drivers and should be updated if connected to other sources. Can take a JDBC 
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
#' @details This function looks in the \code{DBC.COLUMNS} table to determine 
#' whether the Teradata table exists.
#'
#' @param table String vector of name of table to get column names from.
#' @param ... Optional connection settings.
#'
#' @return Returns a boolean indicator of either \code{TRUE} if table exists or 
#' \code{FALSE} if table does not. 
#' 
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdDisk}} for disk usage,
#' \code{\link{tdSpool}} for spool usage, \code{\link{tdCpu}} for CPU 
#' usage, and \code{\link{tdJoin}} for joining tables.
#' 
#' @examples
#' ## NOT RUN (will also result in errors due to user restrictions) ##
#' ## Runs a quick query based on connection profile
#' # tdExists("ICDB_PERSON", username=<username>, password=<password>, db="GCA")
#'
#' ## Runs query using a separately established connection
#' # conn = tdConn(<username>, <password>, db="GCA")
#' # tdExists("ICDB_PERSON", conn=conn)
#'
#' ## Uses same connection, but allows code to find globally.
#' # tdExists("ICDB_PERSON")
#'
#' @export
tdExists = function(table=NULL, ...) {
	
	tmp = paste(substitute(list(table)))[-1]
	if (!exists(tmp)) table=tmp
	if (is.null(table) | all(table=='')) stop("No Teradata table specified.")
	table = strsplit(toupper(table), "\\.")
	if (any(unlist(lapply(table, length))>2)) stop("table name can only have up to 1 period.")
			
	## Connection ##
	conn = tdCheckConn(list(...))
	
	## table ##
	db = toupper(td("select database", conn=conn)[1,1])
	table = do.call("rbind", lapply(table, function(x) {
		if (length(x)==1) {
			return(c(db,x))
		} else if (length(x)==2) {
			return(x)
		} else {
			stop("Problem with the table names.")
		}
	}))
	
	st = paste(paste0("upper(DatabaseName)='", table[,1], "' and upper(TABLENAME)='",table[,2], "'"), collapse=" or ")
	query = sprintf("select top 1 DatabaseName, TableName from DBC.COLUMNS where %s", st)
	tableInfo = td(query, conn=conn)
	foo = nrow(tableInfo)>0
	
	## Connection ##
	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)
	
	return(foo)
}
