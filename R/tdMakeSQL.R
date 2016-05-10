# TODO: Creates SQL code using a template file
#
# Author: Linh Tran
# Date: May 10, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


#' @title tdMakeSQL
#'
#' @description Takes a SQL file and replaces pointers with user declared values. 
#' This code is specifically designed for connectivity to Teradata servers using 
#' OSX at Apple using JDBC drivers and should be updated if connected to other 
#' sources. Can take a JDBC connection object (\code{conn}) if provided. If no 
#' JDBC connection is provided, then a connection is attempted using the 
#' \code{user}, and \code{password} provided. If none is provided, then tries 
#' to locate a connection object (\code{conn}) in the global environment.
#'
#' If a connection profile (e.g. username, password, etc.) is provided, then
#' an attempt is made to connect to Teradata. Once the query is run, the
#' connection is then closed. If a connection object (\code{conn}) is provided
#' to the function (or one is found globally), then the connection remains
#' open.
#'
#' @details
#' \emph{Warning:} This function reads in all lines and parses commands 
#' using "\code{;}". Thus, commands should be separated using that character. 
#' If a literal ";" is desired within the code, an escape character of "\" 
#' should precede it, e.g. \code{where column="\;"}. Pointers should be saved
#' in the file using a "&" prefix. An escape character of "\" can also be used
#' for this if a literal "&" is desired within the code.
#' 
#' @param file File to submit to Teradata.
#' @param outfile Optional file name to output code to. If supplied, then code 
#' will be printed in file rather than console. 
#' @param ... Optional arguments to replace pointers.
#'
#' @return A string vector is returned of the SQL code.
#'
#' @seealso 
#' \code{\link{tdConn}} for connection, \code{\link{tdHead}} for top observations,
#' \code{\link{tdSpool}} for spool usage, and \code{\link{td}} for general 
#' queries
#' 
#' @examples
#' ## NOT RUN ##
#' ## Simply returns the code from the file
#' # tdMakeSQL("file.sql")
#'
#' ## Returns code from file, replacing "&ABTExt" with "Test"
#' # tdMakeSQL("file.sql", ABTExt="Test")
#'
#' ## Returns code from file, replacing "&ABTExt" with "Test" and "&baseExt" with "ing"
#' # tdMakeSQL("file.sql", ABTExt="Test", baseExt="ing")
#'
#' @export
tdMakeSQL <- function(file=NULL, outfile=NULL, ...) { 

	## Checks ##
	if (is.null(file)) {
		stop("File has to be specified.")
	} else {
		fe = file.exists(file)
		if (!fe) stop(paste(file, "not found."))
	}

	## Queries ##
	queries = paste(readLines(file), collapse="\n")
	args = list(...)
	if (length(args)>0) {
		for(i in 1:length(args)) {
			if (grepl(paste0("\\\\&(*SKIP)(*FAIL)|\\&", names(args[i])), queries, ignore.case = TRUE)) {
				queries = gsub(paste0("\\\\&(*SKIP)(*FAIL)|\\&", names(args[i])), args[i], queries, ignore.case = TRUE)			
			} else {
				warning("Pointer", paste(names(args[i]), "not found in text."))
			}
		}
	}
	
	if(!is.null(outfile)) {
		sink(outfile)
		cat(queries)
		sink()
		invisible(NULL)
	} else {
		return(queries)
	}
	
}

