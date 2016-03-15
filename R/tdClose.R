# TODO: Close Teradata connection
# 
# Author: Linh Tran
# Date: Mar 15, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdClose
#'
#' @description Closes connection to the Teradata server. If none exists,
#' then does nothing. This code is specifically designed for connectivity
#' to Teradata servers using OSX at Apple using JDBC drivers and should be updated if
#' connected to other sources. 
#'
#' Takes either a connection object provided, or looks for one globally.
#' 
#' @param conn \code{JDBCConnection} Connection object.
#'
#' @return An \code{\link{invisible}} object is returned, indicating success or failure.
#'
#' @seealso 
#' \\code{\link{tdConn}} for Teradata connection, \code{\link{td}} for Teradata queries.
#' 
#' @examples
#' ## NOT RUN ##
#' ## Connect to default data warehouse and data base
#' # conn = tdConn(<username>, <password>)
#'
#' ## Close connection
#' # tdClose()
#' 
#' @export
tdClose = function(conn=NULL) {
	
	## CHECKS ##
	if(!is.null(conn) & class(conn)!="JDBCConnection") stop('Connection object needs to be JDBCConnection class.')
	if(is.null(conn)) {
		conn = try(get("conn", envir=.GlobalEnv), TRUE)
		if(inherits(conn, "try-error")) stop("No connection detected.") 
	}
		
	## CLOSES ##
	loadNamespace('DBI')
	rs = DBI::dbDisconnect(conn)
	
	invisible(rs)
	
}

