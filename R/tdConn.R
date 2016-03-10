# TODO: Function to connect to Teradata
#
# Author: Linh Tran
# Date: Dec 10, 2015
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdConn
#'
#' @description Checks for a connection to the Teradata server. If none exists,
#' tries to establish one. This code is specifically designed for connectivity
#' to Teradata servers using OSX at Apple using JDBC drivers and should be updated if
#' connected to other sources. If no JDBC connection is provided (\code{tdConn}),
#' then a connection is attempted using the \code{user}, and \code{password} provided.
#'
#' @details Uses the v15.10.00.33 release (12 Jan 2016) tdgssconfig.jar and
#' terajdbc4.jar drivers.
#'
#' @param username Connection user name.
#' @param password Connection password.
#' @param addr String containing address of database to connect to. By default, is
#' \emph{jdbc:teradata://megadew.corp.apple.com/charset=utf8}.
#' @param db Name of database to connect to. By default, is \emph{CDM_Special}.
#' @param classPath The location of the JDBC drivers. By default, will use the drivers included
#' in the package.
#' @param conn \code{DBIConnection} object with established connection to the RDMBS.
#' Only used internally to check connection and establish a connection if none exists.
#'
#' @return A \code{RJDBC} connection object is returned.
#'
#' @examples
#' ## NOT RUN ##
#' ## Connect to default data warehouse and data base
#' # conn = tdConn(<username>, <password>)
#'
#' ## Connect to data warehouse using different data base
#' # conn = tdConn(<username>, <password>, db='ADM_AMR')
#'
#' ## Connect to different data warehouse than default
#' # conn = tdConn(<username>, <password>, addr="jdbc:teradata://redwood.corp.apple.com")
#'
#' @export
tdConn = function(username=NULL, password=NULL, addr="jdbc:teradata://megadew.corp.apple.com/charset=utf8", db=ifelse(addr=="jdbc:teradata://megadew.corp.apple.com/charset=utf8,tmode=ansi", "CDM_Special", NULL), classPath=NULL, conn=NULL) {

	## CHECKS ##
	if(is.null(conn) & (is.null(username) | is.null(password) | is.null(addr))) stop('No connection or profile provided.')
	if(!is.null(conn) & class(conn)!="JDBCConnection") stop('Connection object needs to be JDBCConnection class.')
	if(!is.null(conn) & class(conn)=="JDBCConnection") return(conn)

	## CONNECTION STRING ##
	if (is.null(conn)) {
		pkg = require(RJDBC)
		if(!pkg) {
			stop("Package RJDBC not found.")
		} else {
			if(is.null(classPath)) {
				pkg = find.package('cdmApple')
				Sys.setenv(CLASSPATH = paste(paste0(pkg, c('/drv/tdgssconfig.jar', '/drv/terajdbc4.jar')), collapse=':'))
			} else {
				if (grepl("/$", classPath)) classPath = gsub("/$", "", classPath)
				Sys.setenv(CLASSPATH = paste(paste0(classPath, c('/tdgssconfig.jar', '/terajdbc4.jar')), collapse=':'))
			}
			drv = JDBC("com.teradata.jdbc.TeraDriver")
			if (is.null(db) | db=='') {
				st = addr
			} else if (grepl('[[:alpha:]]/', addr)) {
				if (grepl('[[:alnum:]]$', addr)) {
					st = paste0(addr, ',database=', db)
				} else st = paste0(addr, 'database=', db)
			} else {
				st = paste0(addr, '/database=', db)
			}
		}

		## CONNECTION ##
		conn = dbConnect(drv, st, username, password)
		attr(conn, "tmpConnection") = FALSE

		return(conn)

	} else stop('Unknown bug preventing connection.')

}
