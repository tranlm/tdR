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
#' ## Uses same connection, but allows code to find globally. Also done for multiple tables.
#' # tdNames(c("ICDB_PERSON", "ICDB_PERSON_X"))
#'
#' @export
tdNames = function(table=NULL, ...) {
	
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
	if (nrow(tableInfo)==0) stop(paste("No table details found for:", paste(paste(table[,1], table[,2], sep="."), collapse=", ")))
	tableMissing = NULL	
	for(i in 1:nrow(table)) if (!table[i,2] %in% toupper(tableInfo$TableName)) tableMissing = c(tableMissing, table[i,2])
	if (!is.null(tableMissing)) warning(paste("The following table were not found:", paste(tableMissing, collapse=", ")))
	
	## Primary / secondary indices ##
	tableInfo$Index = NA
	for(i in 1:nrow(table)) {
		tablehow = try(DBI::dbGetQuery(conn, sprintf("show table %s.%s", table[i,1], table[i,2]))[1,1], TRUE)
		if (!inherits(tablehow, "try-error")) {
			tmp = substring(tablehow, regexpr("PRIMARY INDEX \\(", tablehow)+14)
			m = gregexpr("\\([^)]*\\)", tmp)
			indicies = regmatches(tmp, m)[[1]]
			indicies = lapply(indicies, function(x) {
				tmp = unlist(strsplit(substr(x, 2, nchar(x)-1), "\\,"))
				tmp = gsub("^*[[:space:]]|[[:space:]]*$", "", tmp)
				return(tmp)
				})
			keys = lapply(indicies, function(x) unlist(strsplit(x, "\\,")))
			prim.keys = gsub("^*[[:space:]]|[[:space:]]*$", "", keys[[1]])
			if (length(keys)>1) {
				sec.keys = gsub("^*[[:space:]]|[[:space:]]*$", "", keys[[2]])
			} else sec.keys=NULL
			idx = toupper(tableInfo$DatabaseName)==table[i,1] & toupper(tableInfo$TableName)==table[i,2]
			tableInfo$Index[idx] = ifelse(tableInfo$ColumnName[idx] %in% prim.keys, 1, ifelse(tableInfo$ColumnName[idx] %in% sec.keys, 2, 0))
		}
	}
	tableInfo = tableInfo[order(tableInfo$DatabaseName, tableInfo$TableName),]
	
	## Connection ##
	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)
	
	return(tableInfo)
}
