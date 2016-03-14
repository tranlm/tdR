# TODO: Grabs user disk usage from Teradata
#
# Author: Linh Tran
# Date: Mar 10, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdDisk
#'
#' @description Queries Teradata for disk space used. This code is specifically
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
#' @param user Username to grab CPU use from. Defaults to the \code{username}
#' used in the Teradata connection.
#' @param ... Optional connection settings.
#'
#' @return A \code{\link{data.frame}} object is returned with all of the Teradata
#' query information of the specified date.
#'
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdCpu}} for CPU usage,
#' \code{\link{tdSpool}} for spool usage, and \code{\link{td}} for general 
#' queries
#' 
#' @examples
#' ## NOT RUN ##
#' ## Connect to default data warehouse and data base
#' # tdDisk(<username>, <password>)
#'
#' ## Runs query using a separately established connection
#' # conn = tdConn(<username>, <password>)
#' # tdDisk(conn=conn)
#'
#' ## Uses same connection, but allows code to find globally
#' # tdDisk()
#'
#' @export
tdDisk = function(user="user", ...) {

	## Connection ##
	conn = tdCheckConn(list(...))

	query = sprintf(
			"select
				trim(DATABASENAME),
				trim(TABLENAME),
				sum(CURRENTPERM)/(1024**3) ACTUALSPACE,
				max(CURRENTPERM)*(hashamp()+1)/(1024**3) EFFECTIVESPACE,
				hashamp() + 1 as NumAMPs
				from DBC.TABLESIZE a
				where exists
				(select 1 from DBC.TABLES b
				where a.DATABASENAME = b.DATABASENAME
				and a.TABLENAME = b.TABLENAME
				and b.CREATORNAME = %s)
				group by 1, 2;",
			user)
	tableInfo = td(query, conn=conn)

	## Connection ##
	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)

	return(tableInfo)
}


