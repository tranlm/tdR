# TODO: Add description
#
# Author: Linh Tran
# Date: Dec 11, 2015
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdJoin
#'
#' @description
#' Takes two (or more) Teradata EDW tables using a JDBC connection
#' object via the \code{RJDBC} package and merges them together.
#' This code is specifically designed for connectivity to Teradata servers
#' using OSX at Apple using JDBC drivers and should be updated if
#' connected to other sources.
#'
#' @details
#' By default, the code tries to do joins starting from Table 1 going up.
#' So if, for example, three tables are provided for inner joins, then Table 1
#' and Table 2 will first be inner joined, and the resulting output will
#' then be inner joined with Table 3. If a left join is desired for the
#' three tables, then Table 2 will be left joined to Table 1 and Table 3 will
#' then be left joined with the resulting table.
#'
#' If desired, column names for each table can be provided to merge together.
#' By default, the code will try to use all columns of the tables provided.
#' All tables will be searched for duplicate column names. If any exists,
#' then copies will be renamed with a suffix of \code{_copyX} where \code{X}
#' represents the number of copies. If an index name merging by has copies
#' across the tables, then only one index name is kept.
#'
#' If a connection profile (e.g. username, password, etc.) is provided, then
#' an attempt is made to connect to Teradata. Once the query is run, the
#' connection is then closed. If a connection object (\code{conn}) is provided
#' to the function (or one is found globally), then the connection remains
#' open.
#'
#' @param tdfO Name of resulting Teradata to output.
#' @param tdf1 Name of first Teradata table to merge.
#' @param tdf2 Name of second Teradata table to merge.
#' @param index1 Name of index from first table to merge by.
#' @param index2 Name of index from second table to merge by.
#' @param col1 Name of columns from first table to merge.
#' @param col2 Name of columns from second table to merge.
#' @param joinType Type of merge to perform. Needs to be one of following: \code{inner,
#' left outer, right outer, full outer}.
#' @param conn (Optional) Connection object for Teradata.
#' @param username (Optional) Connection user name.
#' @param password (Optional) Connection password.
#' @param addr (Optional) String containing address of database to connect to.
#' By default, is \emph{jdbc:teradata://megadew.corp.apple.com/charset=utf8}.
#' @param db Name of database to connect to. By default, is \emph{CDM_Special}.
#' @param classPath The location of the JDBC drivers. By default, will use the
#' drivers included in the package.
#' @param ... Additional \code{tdfX} and \code{indexX} to merge, where \code{X} is the count.
#'
#' @return The code creates the data table on the Teradata server via the
#' \code{JDBCConnection} object. Names of each table created are returned.
#'
#' @examples
#' ## NOT RUN ##
#' ## With connection pre-established, inner join on table ##
#' # conn = tdConn(<username>, <password>)
#' # tdJoin(<outputTable>, <inputTable1>, <inputTable2>, <index1>, <index2>)
#'
#' ## inner join on table with select columns ##
#' # tdJoin(<outputTable>, <inputTable1>, <inputTable2>, <index1>, <index2>, joinType="left")
#'
#' ## left join on table ##
#' # tdJoin(<outputTable>, <inputTable1>, <inputTable2>, <index1>, <index2>, joinType="left")
#'
#' @export
tdJoin = function(tdfO, tdf1, tdf2, index1, index2, col1=NULL, col2=NULL, joinType="inner", conn=NULL, ...) {

	## CHECKS ##
	args = list(...)
	jtypes = c("inner", "left outer", "right outer", "full outer")
	if(missing(tdf1) | missing(tdf2) | missing(index1) | missing(index2) | length(grep("tdf", args)) != length(grep("index", args))) stop("Both tables and indices (to match by) need to be specified.")
	if(missing(tdfO)) stop("Output table name needs to be specified.")
	if (!joinType %in% jtypes) stop(gettextf("Unknown 'joinType'. Must be one of %s", paste(jtypes, collapse = ",")))
	tdf1 = toupper(tdf1)
	tdf2 = toupper(tdf2)
	index1 = toupper(index1)
	index1 = toupper(index1)

	## Connection ##
	conn = tdCheckConn(list(...))

	## TABLE CHECK ##
	cat("Checking tables...")
	if(dbExistsTable(conn, tdfO)) {
		if (	attr(conn, "tmpConnection")) dbDisconnect(conn)
		stop(paste0(tdfO, " already exists."))
	}

	## COLUMN NAMES ##
	tables = c(tdf1=tdf1, tdf2=tdf2, args[grep("tdf", names(args))])
	tableNames = as.data.frame(do.call("rbind", lapply(tables, function(x) unlist(strsplit(x, split=".", fixed=TRUE)))))
	for(i in 1:2) tableNames[i] = as.character(tableNames[[i]])
	names(tableNames) = c("DatabaseName", "TableName")
	txt = gettextf(
				"SELECT DatabaseName, TableName, ColumnName
				FROM DBC.columns
				WHERE DatabaseName  in ('%s')
				AND TableName in ('%s')",
				paste(tableNames$DatabaseName, collapse="', '"), paste(tableNames$TableName, collapse="', '"))
	tableInfo = td(txt, conn=conn)
	for(i in 1:3) tableInfo[i] = toupper(sub("\\s+$", "", tableInfo[[i]], perl = TRUE))

	## INDICIES ##
	indicies = toupper(unlist(c(index1=index1, index2=index2, args[grep("index", names(args))])))
	indexMissing = NULL
	for(i in 1:nrow(tableNames)) {
		tmpColumns = subset(tableInfo, toupper(DatabaseName)==toupper(tableNames$DatabaseName[i]) & toupper(TableName)==toupper(tableNames$TableName[i]))$ColumnName
		if (!toupper(indicies[i]) %in% toupper(tmpColumns)) indexMissing = c(indexMissing, indicies[i])
	}
	if (!is.null(indexMissing)) {
		if (	attr(conn, "tmpConnection")) dbDisconnect(conn)
		stop(gettextf("The following indicies were not found in the tables: %s", paste(indexMissing, collapse=", ")))
	}
	# renames duplicate indicies
	if (length(indicies)!=length(unique(indicies))) {
		indiciesTmp1 = indiciesTmp2 = indicies
		for(i in 2:length(indicies)) {
			if(indicies[i] %in% indicies[1:(i-1)]) {
				indiciesTmp1[i] = paste0(indicies[i], " ", indicies[i], "_copy", sum(indicies[1:(i-1)]==indicies[i]))
			}
		}
		for(i in 1:nrow(tableNames)) {
			tableInfo$ColumnName[toupper(tableInfo$DatabaseName)==toupper(tableNames$DatabaseName[i]) & toupper(tableInfo$TableName)==toupper(tableNames$TableName[i]) & toupper(tableInfo$ColumnName)==toupper(indicies[i])] = indiciesTmp1[i]
		}
	}

	## VARIABLES ##
	mergeVariables = NULL
	for(i in 1:(length(indicies))) {
		tmpVariables = subset(tableInfo, toupper(DatabaseName)==toupper(tableNames$DatabaseName[i]) & toupper(TableName)==toupper(tableNames$TableName[i]))$ColumnName
		copies = grep("(_copy[0-9])$", tmpVariables)
		if(length(copies)>0) {
			tmpVariables = tmpVariables[-copies]
		}
		mergeVariables = c(mergeVariables, paste(paste0(letters[i], ".", tmpVariables, " ", tmpVariables), collapse=", "))
	}
	cat("good.\n")

	## JOINS ##
	cat("Joining tables...")
	mergeStatement = NULL
	for(i in 2:(length(indicies))) {
		mergeStatement = paste(mergeStatement, gettextf("%s join %s %s on a.%s=%s.%s\n", joinType, paste(tableNames[i,], collapse="."), letters[i], indicies[1], letters[i], indicies[i]))
	}
	txt = gettextf(paste(
			"create table %s
			as (select %s from %s a
			%s)
			with data primary index (%s)"),
			tdfO, paste(mergeVariables, collapse=", "), paste(tableNames[1,], collapse="."), mergeStatement, indicies[1])
	res = td(txt, conn=conn)
	cat("done.\n")

	## Connection ##
	if (	attr(conn, "tmpConnection")) dbDisconnect(conn)

	invisible(tdfO)

}


