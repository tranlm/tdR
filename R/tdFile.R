# TODO: Submits a SQL file to run in Teradata
#
# Author: Linh Tran
# Date: Mar 10, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdFile
#'
#' @description Submits a SQL file to Teradata to run. This code is specifically
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
#' \emph{Warning:} This function rads in all lines and parses commands 
#' using "\code{;}". Thus, commands should be separated using that character. 
#' If a literal ";" is desired within the code, an escape character of "\" 
#' should precede it, e.g. \code{where column="\;"}. 
#' 
#' @param file File to submit to Teradata. 
#' @param ... Optional connection settings.
#'
#' @return An \code{\link{invisible}} object is returned indicating whether 
#' the file ran successfully.
#'
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdHead}} for top observations,
#' \code{\link{tdSpool}} for spool usage, and \code{\link{td}} for general 
#' queries
#' 
#' @examples
#' ## NOT RUN ##
#' ## Connect to default data warehouse and data base
#' # tdFile("file.sql", <username>, <password>)
#'
#' ## Runs query using a separately established connection
#' # conn = tdConn(<username>, <password>)
#' # tdFile("file.sql", conn=conn)
#'
#' ## Uses same connection, but allows code to find globally
#' # tdFile("file.sql")
#'
#' @export
tdFile = function(file=NULL, ...) {

	## Checks ##
	if (is.null(file)) {
		stop("File has to be specified.")
	} else {
		fe = file.exists(file)
		if (!fe) stop(paste(file, "not found."))
	}
	
	## Connection ##
	conn = tdCheckConn(list(...))

	## Queries ##
	queries = paste(readLines(file), collapse=" ")
	queries = strsplit(queries, "\\\\;(*SKIP)(*FAIL)|\\;", perl=TRUE)[[1]]
	for (i in 1:length(queries)) {
		cat(paste("Query:\n", queries[i], "\n"))
		tmp = td(queries[i], conn=conn)
		if(!is.null(tmp)) print(tmp)
	}
	
	## Connection ##
	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)

	invisible(TRUE)
}


