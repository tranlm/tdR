# TODO: Function to upload data to Teradata
# 
# Author: Linh Tran
# Date: Mar 11, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdUpload
#'
#' @description Uploads data to Teradata tables. Can take a JDBC 
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
#' By default, numeric vectors will be uploaded as an int, while character vectors 
#' will be uploaded as a varchar with the longest character length. For now, if any
#' other \code{class} is included, an error will be thrown.
#' 
#' In order to keep the required Java memory down, data is uploaded in batches  
#' at a time. This can be configured to be as many (or little observations) as desired.
#' 
#' @param data \code{link{data.frame}} containing data to upload. This must be in 
#' the same column order as the Teradata table.
#' @param where String statement to subset table with.
#' @param batchSize Number of rows to upload simultaneously.
#'  
#' @param ... Optional connection settings.
#'
#' @return Returns an invisible objecting containing the count of the number of rows uploaded. 
#' 
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdNames}} for table names,
#' \code{\link{td}} for general queries, \code{\link{tdCpu}} for CPU 
#' usage, and \code{\link{tdJoin}} for joining tables.
#' 
#' 
#' @export
tdUpload = function(data=NULL, table=NULL, batchSize=2500, ...) {

	supportedTypes = c("numeric", "character", "Date", "POSIXct", "POSIXt")
	tmp = unlist(lapply(sapply(data, class), function(x) return(x[1])))
	table = strsplit(toupper(table), "\\.")
	if (any(unlist(lapply(table, length))>2)) stop("Table names can only have up to 1 period.")
	
	if (any(!tmp %in% supportedTypes)) stop("Unsupported data type included.")
	
	## Connection ##
	conn = tdCheckConn(list(...))
	
	## table ##
	db = td("select database;", conn=conn)[1,1]
	table = do.call("rbind", lapply(table, function(x) {
		if (length(x)==1) {
			return(c(db,x))
		} else if (length(x)==2) {
			return(x)
		} else {
			stop("Problem with the table names.")
		}
	}))
	table = paste(table[1,1], table[1,2], sep=".")
	if(!tdExists(table)) stop("Table not found.")
	
	ps = .jcall(conn@jc, "Ljava/sql/PreparedStatement;", "prepareStatement", sprintf("insert into %s values(%s)", table, paste(rep("?", ncol(data)), collapse=",")))
	for(i in 1:nrow(data)) {
		for(j in 1:ncol(data)) {
			if (class(data[[j]])=="numeric") {
				.jcall(ps,"V", "setDouble", as.integer(j), data[i,j])			
			} else {
				.jcall(ps,"V", "setString", as.integer(j), as.character(data[i,j]))			
			}
		}
		.jcall(ps,"V", "addBatch")
		if (i %% batchSize == 0 | i==nrow(data)) {
			loadResult = .jcall(ps,"[I","executeBatch")
			message(sprintf("%d rows uploaded", sum(loadResult)))
		}
	}
	.jcall(ps,"V","close")
	rowsUploaded = tdRows(table)
	
	## Connection ##
	if (	attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)
	
	invisible(rowsUploaded)
	
}

