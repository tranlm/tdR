# TODO: Function to grab Teradata CPU usage.
#
# Author: Linh Tran
# Date: Dec 12, 2015
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdCpu
#'
#' @description Queries Teradata for CPU use. Can take a JDBC connection
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
#' @param user Username to grab CPU use from. Defaults to the \code{username}
#' used in the Teradata connection.
#' @param date Date desired for query information. Defaults to today's date. If
#' overwritten, can be in either the date format for R or the Teradata format
#' YYMMDD.
#' @param ... Optional connection settings.
#'
#' @return A \code{\link{data.frame}} object is returned with the Teradata query
#' information of the specified date.
#'
#' @seealso
#' \code{\link{tdConn}} for connection, \code{\link{tdDisk}} for disk usage,
#' \code{\link{tdSpool}} for spool usage, and \code{\link{td}} for general
#' queries
#'
#' @examples
#' ## NOT RUN ##
#' ## Connect to default data warehouse and data base
#' # tdCpu(<username>, <password>)
#'
#' ## Runs query using a separately established connection
#' # conn = tdConn(<username>, <password>)
#' # tdCpu(conn=conn)
#'
#' ## Uses same connection, but allows code to find globally
#' # tdCpu()
#'
#' @export
tdCpu = function(user="user", date=as.Date(Sys.time()), ...) {

	## Connection ##
	conn = tdCheckConn(list(...))

	query = sprintf(
	"SELECT starttime
		, ElapsedTime
		, TotalIOCount                   TotalIO_ct
		, AMPCPUTime
		, ParserCPUTime
		, AMPCPUTime+ParserCPUTime       TotalCPUTime
		, SpoolUsage/(1024*1024*1024)    Spool_GB
		, 100-(nullifzero(AMPCPUTime/hashamp())/(MaxAMPCPUTime)*100) as Skew_Factor
		, Querytext
		, errortext
		FROM dbc.qrylog
			where cast(starttime as date) = '%s' and username = %s
		order by 1 asc;",
	date, user)
	tableInfo = td(query, conn=conn)

	## Connection ##
	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)

	return(tableInfo)
}
