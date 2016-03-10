# TODO: Function to grab Teradata CPU usage.
#
# Author: Linh Tran
# Date: Dec 12, 2015
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title td.cpu
#'
#' @description Queries Teradata for CPU use. This code is specifically designed
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
#' @param user Username to grab CPU use from. Defaults to the \code{username}
#' used in the Teradata connection.
#' @param date Date desired for query information. Defaults to today's date. If
#' overwritten, should be in the format YYMMDD.
#' @param conn (Optional) Connection object for Teradata.
#' @param username (Optional) Connection user name.
#' @param password (Optional) Connection password.
#' @param addr (Optional) String containing address of database to connect to.
#' By default, is \emph{jdbc:teradata://megadew.corp.apple.com/charset=utf8}.
#' @param db Name of database to connect to. By default, is \emph{CDM_Special}.
#' @param classPath The location of the JDBC drivers. By default, will use the
#' drivers included in the package.
#'
#' @return A \code{data.frame} object is returned with the Teradata query
#' information of the specified date.
#'
#' @examples
#' ## NOT RUN ##
#' ## Connect to default data warehouse and data base
#' # td.cpu(<username>, <password>)
#'
#' ## Runs query using a separately established connection
#' # conn = tdConn(<username>, <password>)
#' # td.cpu(conn=conn)
#'
#' ## Uses same connection, but allows code to find globally
#' # td.cpu()
#'
#' @export
td.cpu = function(user="user", date=format(as.Date(Sys.time()), "%y%m%d"), ...) {

	## Connection ##
	conn = tdCheckConn(list(...))

	query = sprintf(
	"SELECT starttime
		, TotalIOCount                   TotalIO_ct
		, AMPCPUTime
		, ParserCPUTime
		, AMPCPUTime+ParserCPUTime       TotalCPUTime
		, SpoolUsage/(1024*1024*1024)    Spool_GB
		, 100-(nullifzero(AMPCPUTime/hashamp())/(MaxAMPCPUTime)*100) as Skew_Factor
		, Querytext
		, errortext
		FROM dbc.qrylog
			where cast(starttime as date) = 1%s and username = %s
		order by 1 asc;",
	date, user)
	tableInfo = td(query, conn=conn)

	## Connection ##
	if (	attr(conn, "tmpConnection")) dbDisconnect(conn)

	return(tableInfo)
}


