# TODO: Function to grab column names of Teradata table.
# 
# Author: Linh Tran
# Date: Dec 12, 2015
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdNames
#'
#' @description Gets column names from Teradata tables. Can take a JDBC 
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
#' trying to query for indicies will result in \code{Error 3853}. This is a part of
#' the user restrictions and can be circumvented by creating a new subset of 
#' the table and querying that subset. If the index is unable to be determined, 
#' a value of \code{NA} will be returned.
#' 
#' @param table String vector of name of table to get column names from.
#' @param ... Optional connection settings.
#'
#' @return Returns a \code{\link{data.frame}} object with the following items:  
#' \itemize{
#' 	\item{"DatabaseName"}{Database name}
#' 	\item{"TableName"}{Table name}
#' 	\item{"ColumnName"}{Column name}
#'  	\item{"ColumnFormat"}{Column format}
#' 	\item{"ColumnType"}{Column type.}
#' 	\item{"ColumnLength"}{Column length.}
#' 	\item{"Index"}{Indicator of whether column is a primary index (\code{1}) or secondary index (\code{2})}
#' }
#' 
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdDisk}} for disk usage,
#' \code{\link{tdSpool}} for spool usage, \code{\link{tdCpu}} for CPU 
#' usage, and \code{\link{tdJoin}} for joining tables.
#' 
#' @examples
#' ## NOT RUN (will also result in errors due to user restrictions) ##
#' ## Runs a quick query based on connection profile
#' # tdNames("ICDB_PERSON", username=<username>, password=<password>, db="GCA")
#'
#' ## Runs query using a separately established connection
#' # conn = tdConn(<username>, <password>, db="GCA")
#' # tdNames("ICDB_PERSON", conn=conn)
#'
#' ## Uses same connection, but allows code to find globally. 
#' # tdNames("ICDB_PERSON")
#'
#' @export
tdNames = function(table=NULL, ...) {
	
	tmp = try(eval(table), TRUE)
	if (inherits(tmp, "try-error")) tmp = paste(substitute(list(table)))[-1]
	if (!exists(tmp)) table=tmp
	if (is.null(table) | all(table=='')) stop("No Teradata table specified.")
	table = strsplit(toupper(table), "\\.")[[1]]
	if (length(table)>2) stop("table name can only have up to 1 period.")
			
	## Connection ##
	conn = tdCheckConn(list(...))

	## Query ##
	qry = paste("where", ifelse(length(table)==1, sprintf("upper(TableName)='%s'", table[1]), ifelse(length(table)==2, sprintf("upper(DatabaseName)='%s' AND upper(TableName)='%s'", table[1], table[2]), "")))
	tableInfo = td(sprintf("select trim(ColumnName) names FROM DBC.COLUMNS %s;", qry), conn=conn)
	if (nrow(tableInfo)==0) stop(paste("No table details found for:", paste(paste(table[,1], table[,2], sep="."), collapse=", ")))
	
	## Connection ##
	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)
	
	return(tableInfo$names)
}
