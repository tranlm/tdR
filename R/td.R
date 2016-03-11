# TODO: Function to send queries and grab results (if any)
#
# Author: Linh Tran
# Date: Mar 10, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title td
#'
#' @description Sends queries to Teradata. This code is specifically designed
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
#' @param query Query string to send to Teradata.
#' @param conn (Optional) Connection object for Teradata.
#' @param username (Optional) Connection user name.
#' @param password (Optional) Connection password.
#' @param addr (Optional) String containing address of database to connect to.
#' By default, is \emph{jdbc:teradata://megadew.corp.apple.com/charset=utf8}.
#' @param db (Optional) Name of database to connect to. By default, is \emph{CDM_Special}.
#' @param classPath (Optional) The location of the JDBC drivers. By default, will use the
#' drivers included in the package.
#'
#' @return If no data is returned from query, then an \code{\link{invisible}} 
#' object is returned. Otherwise, a \code{\link{data.frame}} object with all 
#' data queried will be returned.
#'
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdDisk}} for disk usage,
#' \code{\link{tdSpool}} for spool usage, \code{\link{tdCpu}} for CPU 
#' usage, and \code{\link{tdJoin}} for joining tables.
#' 
#' @examples
#' ## NOT RUN ##
#' ## Runs a quick query based on connection profile
#' # td("select count(*) from ICDB_PERSON", username=<username>, password=<password>, db="GCA")
#'
#' ## Runs query using a separately established connection
#' # conn = tdConn(<username>, <password>, db="GCA")
#' # td("select count(*) from ICDB_PERSON", conn=conn)
#'
#' ## Uses same connection, but allows code to find globally
#' # td("select count(*) from ICDB_PERSON")
#'
#' @export
td = function(query="", ...) {

	if (query=="") stop("No query statement given.")

	## Connection ##
	conn = tdCheckConn(list(...))

	## Query ##
	rs = try(dbGetQuery(conn, query), TRUE)

	## Connection ##
	if (attr(conn, "tmpConnection")) dbDisconnect(conn)

	## Output ##
	if(grepl("invalid value from generic function 'fetch'", rs[1])) {
		invisible(NULL)
	} else if (!inherits(rs, "try-error")) {
		return(rs)
	} else {
		err = attr(rs, "condition")
		pos = regexpr('\\(\\[Teradata Database\\]|\\[Teradata', err)
		rs = substring(err, pos)
		stop(paste("\nQuery:", query, "\n", rs))
	}

}
