% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tdConn.R
\name{tdConn}
\alias{tdConn}
\title{tdConn}
\usage{
tdConn(username = getOption("tdPassword"), password = NULL,
  addr = getOption("tdAddr"), db = "", classPath = NULL, conn = NULL)
}
\arguments{
\item{username}{Connection user name. By default, will try to search for a global
environment variable called \code{tdPassword}. If found, will use those credentials.}

\item{password}{Connection password. By default, will try to search for a global 
environment variable called \code{tdPassword}. If found, will use those credentials.}

\item{addr}{String containing address of database to connect to. By default, will 
try to search for a global environment variable called \code{tdAddr}. If found, will
use those credentials.}

\item{db}{Name of database to connect to.}

\item{classPath}{The location of the JDBC drivers. By default, will use the drivers included
in the package.}

\item{conn}{\code{DBIConnection} object with established connection to the RDMBS.
Only used internally to check connection and establish a connection if none exists.}
}
\value{
A \code{RJDBC} connection object is returned.
}
\description{
Checks for a connection to the Teradata server. If none exists,
tries to establish one. If no JDBC connection is provided (\code{tdConn}),
then a connection is attempted using the \code{user}, and \code{password} provided.
}
\details{
If desired, you can define your username and password in the .Rprofile
file using the command \code{options(tdPassword = c(<username>="<password>"))}, which
will then automatically assign the password in the background each time R is started.
This then allows you to connect without having to enter your username and password
manually each time you connect.

The JDBC driver included uses the v15.10.00.33 release (12 Jan 2016)
tdgssconfig.jar and terajdbc4.jar drivers.
}
\examples{
## NOT RUN ##
## Connect to default data warehouse and data base
# conn = tdConn()

## Connect to data warehouse using different credentials and data base
# conn = tdConn(<username>, <password>, db='ADM_AMR')

## Connect to different data warehouse than default
# conn = tdConn(<username>, <password>, addr="jdbc:teradata://new.warehouse.com")

}
\seealso{
\code{\link{td}} for Teradata queries, \code{\link{tdDisk}} for disk usage,
\code{\link{tdSpool}} for spool usage, \code{\link{tdCpu}} for CPU
usage, and \code{\link{tdJoin}} for joining tables.
}

