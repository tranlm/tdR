% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tdCpu.R
\name{tdCpu}
\alias{tdCpu}
\title{tdCpu}
\usage{
tdCpu(user = "user", date = as.Date(Sys.time()), ...)
}
\arguments{
\item{user}{Username to grab CPU use from. Defaults to the \code{username}
used in the Teradata connection.}

\item{date}{Date desired for query information. Defaults to today's date. If
overwritten, can be in either the date format for R or the Teradata format
YYMMDD.}

\item{...}{Optional connection settings.}
}
\value{
A \code{\link{data.frame}} object is returned with the Teradata query
information of the specified date.
}
\description{
Queries Teradata for CPU use. Can take a JDBC connection
object (\code{conn}) if provided. If no JDBC connection is provided, then
a connection is attempted using the \code{user}, and \code{password} provided.
If none is provided, then tries to locate a connection object (\code{conn})
in the global environment.

If a connection profile (e.g. username, password, etc.) is provided, then
an attempt is made to connect to Teradata. Once the query is run, the
connection is then closed. If a connection object (\code{conn}) is provided
to the function (or one is found globally), then the connection remains
open.
}
\examples{
## NOT RUN ##
## Connect to default data warehouse and data base
# tdCpu(<username>, <password>)

## Runs query using a separately established connection
# conn = tdConn(<username>, <password>)
# tdCpu(conn=conn)

## Uses same connection, but allows code to find globally
# tdCpu()

}
\seealso{
\code{\link{tdConn}} for connection, \code{\link{tdDisk}} for disk usage,
\code{\link{tdSpool}} for spool usage, and \code{\link{td}} for general
queries
}

