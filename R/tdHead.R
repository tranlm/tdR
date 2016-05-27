# TODO: Shows first parts of a Teradata table.
# 
# Author: Linh Tran
# Date: Mar 11, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdHead
#'
#' @description Gets the top observations in a Teradata table. This code is specifically 
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
#' @param table A string stating the Teradata table name.
#' @param n A single integer, representing the number of rows desired. Defaults to 10.
#' @param cols Columns desired. Defaults to all columns.
#' @param where String statement to subset table with.
#' @param ... Optional connection settings.
#'
#' @return Returns a \code{\link{data.frame}} of the the Teradata table with the first
#' \code{n} observations. 
#' 
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdNames}} for table names,
#' \code{\link{td}} for general queries, \code{\link{tdCpu}} for CPU 
#' usage, and \code{\link{tdJoin}} for joining tables.
#' 
#' @examples
#' ## NOT RUN (will also result in errors due to user restrictions) ##
#' ## Runs a quick query based on connection profile
#' # tdHead("ICDB_PERSON", username=<username>, password=<password>, db="GCA")
#'
#' ## Runs query using a separately established connection. Selects 20 observations 
#' # from only two columns, subset by even Person_Id. 
#' # conn = tdConn(<username>, <password>, db="GCA")
#' # tdHead("ICDB_PERSON", 20, c("PERSON_ID", "INDIV_ID"), "PERSON_ID mod 2 = 0", conn=conn)
#'
#' ## Uses same connection, but allows code to find globally. Also subsets on PERSON_ID. 
#' # tdHead("ICDB_PERSON")
#'
#' @export
tdHead = function(table=NULL, n=10, cols=NULL, where="", ...) {

	tmp = try(eval(table), TRUE)
	if (inherits("try-error", tmp)) tmp = paste(substitute(list(table)))[-1]
	if (!exists(tmp)) table=tmp
	if (is.null(table) | all(table=='')) stop("No Teradata table specified.")
	if (length(table)>1) stop("Only 1 Teradata table can be supplied.")
	table = strsplit(toupper(table), "\\.")
	if (any(unlist(lapply(table, length))>2)) stop("Table name can only have up to 1 period.")
	
	## Connection ##
	conn = tdCheckConn(list(...))
	
	## Tables ##
	db = td("select database;", conn=conn)[1,1]
	table = do.call("rbind", lapply(table, function(x) {
		if (length(x)==1) {
			return(c(db,x))
		} else if (length(x)==2) {
			return(x)
		} else {
			stop("Problem with the table names.")
		}
	}))

	## Columns ##
	if (is.null(cols)) cols = "*"
	cols = paste(cols, collapse=", ")
	
	## Subset ##
	if (where!="") where = paste("where", where)

	## Observations ##
	tabObs = td(sprintf('select top %d %s from %s.%s %s;', n, cols, table[1,1], table[1,2], where), conn=conn)
	
	## Connection ##
	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)
	
	return(tabObs)
}


