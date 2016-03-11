# TODO: Function to grab column names of Teradata table.
# 
# Author: Linh Tran
# Date: Dec 12, 2015
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdNames
#'
#' @description Gets column names from Teradata tables. This code is specifically 
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
#' @details 
#' Many of the core production tables in Teradata are locked, such that 
#' trying to query them will result in \code{Error 3853}. This is a part of
#' the user restrictions and can be circumvented by creating a new subset of 
#' the table and querying that subset. 
#' 
#' @param tables String vector of names of tables to get column names from.
#' @param conn (Optional) Connection object for Teradata.
#' @param username (Optional) Connection user name.
#' @param password (Optional) Connection password.
#' @param addr (Optional) String containing address of database to connect to.
#' By default, is \emph{jdbc:teradata://megadew.corp.apple.com/charset=utf8}.
#' @param db (Optional) Name of database to connect to. By default, is \emph{CDM_Special}.
#' @param classPath (Optional) The location of the JDBC drivers. By default, will use the
#' drivers included in the package.
#'
#' @return Returns a \code{\link{data.frame}} object with the following items:  
#' \itemize{
#' 	\item{"DatabaseName"}{Database name}
#' 	\item{"TableName"}{Table name}
#' 	\item{"ColumnName"}{Column name}
#'  	\item{"ColumnFormat"}{Column format}
#' 	\item{"ColumnType"}{Column type.}
#' 	\item{"ColumnLength"}{Column length.}
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
#' ## Uses same connection, but allows code to find globally
#' # td("select count(*) from ICDB_PERSON")
#'
#' @export
tdNames = function(tables=NULL, ...) {
	
	if (is.null(tables) | tables=='') stop("No Teradata table specified.")
	tables = strsplit(toupper(tables), "\\.")
	if (any(unlist(lapply(tables, length))>2)) stop("Table name can only have up to 1 period.")
			
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
	
	st = paste(paste0("upper(DatabaseName)='", tables[,1], "' and upper(TABLENAME)='",tables[,2], "'"), collapse=" or ")
	query = sprintf("select
				DatabaseName, 
				TableName, 
				ColumnName, 
				ColumnFormat, 
				ColumnType, 
				ColumnLength
			FROM DBC.COLUMNS
			WHERE %s", st)
	tableInfo = td(query, conn=conn)
	tableInfo$DatabaseName = gsub("[[:space:]]*$", "", tableInfo$DatabaseName)
	tableInfo$TableName = gsub("[[:space:]]*$", "", tableInfo$TableName)
	tableInfo$ColumnName = gsub("[[:space:]]*$", "", tableInfo$ColumnName)
	tableInfo$ColumnFormat = gsub("[[:space:]]*$", "", tableInfo$ColumnFormat)
	
	## Primary / secondary indices ##
	tableInfo$index = NA
	for(i in 1:nrow(tables)) {
		tableShow = try(dbGetQuery(conn, sprintf("show table %s.%s", tables[i,1], tables[i,2])), TRUE)
		if (!inherits(tableShow, "try-error")) {
			tmp = substring(tableShow, regexpr("PRIMARY INDEX \\(", tableShow[1,1])+14)
			m = gregexpr("\\([^)]*\\)", tmp)
			indicies = regmatches(tmp, m)[[1]]
			indicies = lapply(indicies, function(x) {
				tmp = unlist(strsplit(substr(x, 2, nchar(x)-1), "\\,"))
				tmp = gsub("^*[[:space:]]|[[:space:]]*$", "", tmp)
				return(tmp)
				})
					
			prim.keys = unlist(strsplit(substr(tmp, 1, nchar(tmp)-3), "\\,"))
			prim.keys = gsub("^*[[:space:]]|[[:space:]]*$", "", prim.keys)
			
			pos = regexpr("INDEX \\(", tableInfo[1,1])
			all.keys = 
			tableInfo
		}
		pos = 
		prim.key = substring()
	}
	
	
	## Connection ##
	if (	attr(conn, "tmpConnection")) dbDisconnect(conn)
	
	return(tableInfo)
}
