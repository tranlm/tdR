# TODO: Gets percentiles from Teradata table.
#
# Author: Linh Tran
# Date: Mar 11, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdQuantile
#'
#' @description Gets column quantiles from a Teradata table. Can take a JDBC
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
#' This code is CPU intensive, especially for large data tables, as it requires that 
#' the column values be ordered. It is advised to take care when implementing, as user 
#' limits may prevent the code from sucessfully running. If CPU or spool limits are 
#' reached, a workaround could be implemented by first breaking the data table into 
#' smaller subsets and subsequently taking the percentiles over them.
#'
#' The code is really meant for numeric valued columns. If string columns are
#' provided, the code will still run. However, the results will be less interpretable.
#'
#' @param table A string stating the Teradata table name.
#' @param probs Numeric vector of quantiles with values in [0,1]. Defaults to median (i.e. 0.5)
#' @param cols Columns desired. Defaults to all columns.
#' @param where Statement to subset table with.
#' @param ... Optional connection settings.
#'
#' @return Returns a \code{\link{data.frame}} of the the Teradata table with the quantiles
#' specified.
#'
#' @seealso
#' \code{\link{tdConn}} for connection, \code{\link{tdNames}} for table names,
#' \code{\link{td}} for general queries, \code{\link{tdCpu}} for CPU
#' usage, and \code{\link{tdHead}} for top rows in table.
#'
#' @examples
#' ## NOT RUN (will also result in errors due to user restrictions) ##
#' ## Runs a quick query based on connection profile
#' # tdQuantiles("ICDB_PERSON", username=<username>, password=<password>, db="GCA")
#'
#' ## Runs query using a separately established connection. Selects only two columns.
#' # conn = tdConn(<username>, <password>, db="GCA")
#' # tdQuantilesy("ICDB_PERSON", c("PERSON_ID", "INDIV_ID"), conn=conn)
#'
#' ## Uses same connection, but allows code to find globally. Also subsets on PERSON_ID.
#' # tdQuantiles("ICDB_PERSON", where="PERSON_ID mod 2 = 0")
#'
#' @export
tdQuantile = function(table=NULL, probs=0.5, cols=NULL, where="", ...) {

	tmp = try(eval(table), TRUE)
	if (inherits(tmp, "try-error")) tmp = paste(substitute(list(table)))[-1]
	if (!exists(tmp)) table=tmp
	if (is.null(table) | all(table=='')) stop("No Teradata table specified.")
	if (length(table)>1) stop("Only 1 Teradata table can be supplied.")
	table = strsplit(toupper(table), "\\.")[[1]]
	if (length(table)>2) stop("Table name can only have up to 1 period.")
	if (!all(is.numeric(probs))) stop("Quantiles are not numeric.")
	if (!all(probs>=0 & probs<=1)) stop("Quantiles need to be in the range [0,1].")

	## Connection ##
	conn = tdCheckConn(list(...))

	## Tables ##
	table = paste(table, collapse=".")

	## Columns ##
	if (is.null(cols)) {
		cols = tdNames(table, conn=conn)$ColumnName
	}

	## Subset ##
	if (where!="") where = paste("where", where)

	## Quantiles ##
	tabQuantiles = NULL
	for (i in 1:length(cols)) {
		if (where!="") {
			tmpWhere = paste(where, "and", cols[i], "is not null")
		} else {
			tmpWhere = paste("where", cols[i], "is not null")
		}
		nrows = td(sprintf('select cast(count(*) as bigint) from %s %s', table, tmpWhere))[1,1]
		if (nrows>0) {
			Finverse = ceiling((nrows-1)*probs + 1)
			tmpQuantiles = td(sprintf('sel (row_number() over (order by %s) - 1) as percentile, %s from %s %s qualify percentile in (%s);', cols[i], cols[i], table, tmpWhere, paste(Finverse, collapse=", ")), conn=conn)
			tmpQuantiles$ColumnName = cols[i]
			names(tmpQuantiles)[2] = 'value'
			tmpQuantiles = tmpQuantiles[order(tmpQuantiles$percentile),]
			tmpQuantiles$percentile = probs[order(probs)]
			tabQuantiles = rbind(tabQuantiles, tmpQuantiles)
		} else {
			warning(paste(cols[i], "has no non-null values (accounting for where statement)."))
		}
		rm(tmpQuantiles)
	}

	## Connection ##
	if (attr(conn, "tmpConnection")) DBI::dbDisconnect(conn)

	return(tabQuantiles)
}


