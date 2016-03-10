# TODO: Add description
# 
# Author: Linh Tran
# Date: Dec 12, 2015
# Email: Linh_m_tran@apple.com
###############################################################################


#' @export
tdNames = function(tdConnection, tdf) {
	txt =	"SELECT DatabaseName, TableName, ColumnName, ColumnFormat, ColumnType, ColumnLength
			FROM DBC.columns
			WHERE DatabaseName in ('ADM_AMR')
			AND TableName in ('LT_ITUNES_ACTIVITY')"
	tableInfo = dbGetQuery(tdConnection, txt)
	return(tableInfo)
}

