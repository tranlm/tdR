# TODO: Function to send queries and grab results (if any)
#
# Author: Linh Tran
# Date: Mar 10, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title td
#'
#' @description Sends queries to Teradata. Can take a JDBC connection
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
#' A hidden option allows the user to print the SQL code being generated directly
#' to the console without submitting the query to Teradata. Simply specify
#' \code{.tdCat=TRUE} in the global environment before running the code.
#'
#' @param query Query string to send to Teradata.
#' @param ... Optional connection settings.
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

	tdCat = try(get(".tdCat", envir=.GlobalEnv), TRUE)
	if (!inherits(tdCat, "try-error") & tdCat==TRUE) {
		cat("\n", query, "\n")
		rs = NULL
	} else {
		## Connection ##
		conn = tdCheckConn(list(...))

		## Query ##
		rs = try(DBI::dbGetQuery(conn, query), TRUE)

		## Connection ##
		if (attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)

		## Output ##
		if(grepl("invalid value from generic function ", rs[1])) {
			rs = NULL
		} else if (!inherits(rs, "try-error")) {
			return(rs)
		} else {
			err = attr(rs, "condition")
			pos = regexpr('\\(\\[Teradata Database\\]|\\[Teradata', err)
			rs = substring(err, pos)
			if (nchar(query)>800) {
				msg = rs
			} else msg = paste(rs, "Query:\n", query, "\n")
			stop(msg)
		}
	}
	invisible(rs)

}

