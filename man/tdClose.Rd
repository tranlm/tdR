% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tdClose.R
\name{tdClose}
\alias{tdClose}
\title{tdClose}
\usage{
tdClose(conn = NULL)
}
\arguments{
\item{conn}{\code{JDBCConnection} Connection object.}
}
\value{
An \code{\link{invisible}} object is returned, indicating success or failure.
}
\description{
Closes connection to the Teradata server. If none exists,
then does nothing. 

Takes either a connection object provided, or looks for one globally.
}
\examples{
## NOT RUN ##
## Connect to default data warehouse and data base
# conn = tdConn(<username>, <password>)

## Close connection
# tdClose()

}
\seealso{
\\code{\link{tdConn}} for Teradata connection, \code{\link{td}} for Teradata queries.
}

