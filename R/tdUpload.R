# TODO: Function to grab Teradata table rows
# 
# Author: Linh Tran
# Date: Mar 11, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdUpload
#'
#' @description Uploads data to Teradata tables. This code is specifically 
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
#' By default, numeric vectors will be uploaded as an int, while character vectors 
#' will be uploaded as a varchar with the longest character length. For now, if any
#' other \code{class} is included, an error will be thrown.
#' 
#' @param data \code{link{data.frame}} containing data to upload 
#' @param where String statement to subset table with. 
#' @param ... Optional connection settings.
#'
#' @return Returns a count of the number of rows uploaded. 
#' 
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdNames}} for table names,
#' \code{\link{td}} for general queries, \code{\link{tdCpu}} for CPU 
#' usage, and \code{\link{tdJoin}} for joining tables.
#' 
#' 
#' @export
tdUpload = function(data=NULL, table=NULL) {

	stop("Function not working yet.")
#	supportedTypes = c("numeric", "character", "Date", "POSIXct", "POSIXt")
#	tmp = unlist(lapply(sapply(data, class), function(x) return(x[1])))
#	table = strsplit(toupper(table), "\\.")
#	if (any(unlist(lapply(table, length))>2)) stop("Table names can only have up to 1 period.")
#	
#	if (any(!tmp %in% supportedTypes)) stop("Unsupported data type included.")
#	
#	## Connection ##
#	conn = tdCheckConn(list(...))
#	
#	## table ##
#	db = td("select database;", conn=conn)[1,1]
#	table = do.call("rbind", lapply(table, function(x) {
#		if (length(x)==1) {
#			return(c(db,x))
#		} else if (length(x)==2) {
#			return(x)
#		} else {
#			stop("Problem with the table names.")
#		}
#	}))
#	table = paste(table[1,1], table[1,2], sep=".")
#
#	## columns ##
#	tmp2 
#	
#	if(!tdExists(table)) {
#		td("create table 
#	}
#	
#	ps = .jcall(conn@jc, "Ljava/sql/PreparedStatement;", "prepareStatement", sprintf("insert into %s values(%s)", table, paste(rep("?", ncol(data)), collapse=",")))
#	for(i in 1:nrow(aniData)) {
#		.jcall(ps,"V","setString", as.integer(1), aniData$ANI[i])
#		.jcall(ps,"V","addBatch")
#	}
#loadResult = .jcall(ps,"[I","executeBatch")
#cat(sum(loadResult), "records loaded successfully.\n")
#.jcall(ps,"V","close")
#
#	
#	## Connection ##
#	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)
#	
#	return(tmpRows)
}

