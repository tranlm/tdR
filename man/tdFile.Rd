% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tdFile.R
\name{tdFile}
\alias{tdFile}
\title{tdFile}
\usage{
tdFile(file = NULL, ...)
}
\arguments{
\item{file}{File to submit to Teradata.}

\item{...}{Optional connection settings.}
}
\value{
An \code{\link{invisible}} object is returned indicating whether 
the file ran successfully.
}
\description{
Submits a SQL file to Teradata to run. Can take a JDBC
connection object (\code{conn}) if provided. If no JDBC connection is provided,
then a connection is attempted using the \code{user}, and \code{password}
provided. If none is provided, then tries to locate a connection object
(\code{conn}) in the global environment.

If a connection profile (e.g. username, password, etc.) is provided, then
an attempt is made to connect to Teradata. Once the query is run, the
connection is then closed. If a connection object (\code{conn}) is provided
to the function (or one is found globally), then the connection remains
open.
}
\details{
\emph{Warning:} This function reads in all lines and parses commands 
using "\code{;}". Thus, commands should be separated using that character. 
If a literal ";" is desired within the code, an escape character of "\" 
should precede it, e.g. \code{where column="\;"}.
}
\examples{
## NOT RUN ##
## Connect to default data warehouse and data base
# tdFile("file.sql", <username>, <password>)

## Runs query using a separately established connection
# conn = tdConn(<username>, <password>)
# tdFile("file.sql", conn=conn)

## Uses same connection, but allows code to find globally
# tdFile("file.sql")

}
\seealso{
\code{\link{tdConn}} for connection, \code{\link{tdHead}} for top observations,
\code{\link{tdSpool}} for spool usage, and \code{\link{td}} for general 
queries
}

