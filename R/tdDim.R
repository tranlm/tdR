# TODO: Function to grab Teradata table dimensions
# 
# Author: Linh Tran
# Date: Mar 11, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdDim
#'
#' @description Gets the dimensions in Teradata tables. This code is specifically 
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
#' @param tables String vector of names of tables to get table dimensions from.
#' @param where String statement to subset table with. 
#' @param ... Optional connection settings.
#'
#' @return Returns a \code{\link{data.frame}} of the the Teradata table dimensions. 
#' 
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdNames}} for table names,
#' \code{\link{td}} for general queries, \code{\link{tdCpu}} for CPU 
#' usage, and \code{\link{tdJoin}} for joining tables.
#' 
#' @examples
#' ## NOT RUN (will also result in errors due to user restrictions) ##
#' ## Runs a quick query based on connection profile
#' # tdDim("ICDB_PERSON", username=<username>, password=<password>, db="GCA")
#'
#' ## Runs query using a separately established connection
#' # conn = tdConn(<username>, <password>, db="GCA")
#' # tdDim("ICDB_PERSON", conn=conn)
#'
#' ## Uses same connection, but allows code to find globally. Also used for multiple tables.
#' # tdDim(c("ICDB_PERSON", "ICDB_PERSON_X"))
#'
#' @export
tdDim = function(tables=NULL, where="", ...) {
	
	if (is.null(tables) | all(tables=='')) stop("No Teradata table specified.")
	tables = strsplit(toupper(tables), "\\.")
	if (any(unlist(lapply(tables, length))>2)) stop("Table names can only have up to 1 period.")
	
	## Connection ##
	conn = tdCheckConn(list(...))
	
	## Tables ##
	db = td("select database", conn=conn)[1,1]
	tables = do.call("rbind", lapply(tables, function(x) {
		if (length(x)==1) {
			return(c(db,x))
		} else if (length(x)==2) {
			return(x)
		} else {
			stop("Problem with the table names.")
		}
	}))

	## Subset ##
	if (where!="") where = paste("where", where)
	
	tableDim = NULL
	for(i in 1:nrow(tables)) {
		tmpResult = DBI::dbGetQuery(conn, sprintf("SELECT trim(DatabaseName), trim(TableName), count(columnname) as ncol FROM DBC.Columns WHERE upper(DatabaseName)='%s' AND upper(TableName)='%s' group by 1,2;", tables[i,1], tables[i,2]))
		if (nrow(tmpResult)==0) tmpResult = data.frame(DatabaseName=tables[i,1], TableName=tables[i,2], ncol=NA)
		tmpRows = try(DBI::dbGetQuery(conn, sprintf("select cast(count(*) as dec(18,0)) from %s.%s %s;", tables[i,1], tables[i,2], where))[1,1], TRUE)
		tmpResult$nrow = ifelse(inherits(tmpRows, 'try-error'), NA, tmpRows)
		tableDim = rbind(tableDim, tmpResult)
		rm(tmpRows)
	}
	
	## Connection ##
	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)
	
	return(tableDim)
}

