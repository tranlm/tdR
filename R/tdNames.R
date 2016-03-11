# TODO: Function to grab column names of Teradata table.
# 
# Author: Linh Tran
# Date: Dec 12, 2015
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdNames
#'
#' @description Gets column names from Teradata table. This code is specifically designed
#' for connectivity to Teradata servers using OSX at Apple using JDBC drivers
#' and should be updated if connected to other sources. Can take a JDBC connection
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
#' @details Uses the v15.10.00.33 release (12 Jan 2016) tdgssconfig.jar and
#' terajdbc4.jar drivers.
#'
#' @param table Name of table to get column names from.
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
#' \item{"column"}{Column name}
#' \item{"index"}{Whether column is primary or secondary index.}
#' \item{"type"}{Column type in Teradata.}
#' }
#' 
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdDisk}} for disk usage,
#' \code{\link{tdSpool}} for spool usage, \code{\link{tdCpu}} for CPU 
#' usage, and \code{\link{tdJoin}} for joining tables.
#' 
#' @examples
#' ## NOT RUN ##
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
tdNames = function(tdConnection, tdf) {
	txt =	"SELECT DatabaseName, TableName, ColumnName, ColumnFormat, ColumnType, ColumnLength
			FROM DBC.columns
			WHERE DatabaseName in ('ADM_AMR')
			AND TableName in ('LT_ITUNES_ACTIVITY')"
	tableInfo = dbGetQuery(tdConnection, txt)
	return(tableInfo)
}

