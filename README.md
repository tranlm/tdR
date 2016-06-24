# tdR R-package

This is the current version of the tdR R package (version 0.4.3\*). Dependencies for this package include the DBI and RJDBC packages, which depend on rJava.

## How to install from Gitlab ##

To install the tdR package, we recommend cloning the gitlab repository
=======

### Gitlab
With the terminal open, you can install the cdmApple package using the following script.

```shell
git clone git@gitlab.sd.apple.com:cdmApple/tdR.git
R --vanilla CMD INSTALL tdR
```

### Download
Alternatively, the package is available for download. Safari unzips the compressed folder, and will usually add a suffix to the name. The package can then be install with the command

```shell
R --vanilla CMD INSTALL tdR-master-XXXX
```

where XXXX is the suffix added to the end of the uncompressed folder.

### Teradata information
For convenience, you can store your teradata username and password in the .Rprofile file, normally stored in the user home directory. It should be stored as

```shell
options(tdPassword=c(<Teradata Username>="<Teradata Password>"), tdAddr=<Teradata Address String>)
```
So, for example, if your username is b123456, your password is "mypassword", and your address is "my.warehouse.com", you would specify 
```shell
options(tdPassword=c(b123456="mypassword"), tdAddr="jdbc:teradata://my.warehouse.com")
```

An example of the typical R script workflow
=======
```
library(tdR)

## ESTABLISH CONNECTION ##
## nb. Do not change the name if you want subsequent code to discover the connection automatically
conn = tdConn()

## UPLOADING DATA ##
data(mtcars)
XX_data = cbind(mtcars, car=rownames(mtcars)) 
tdCreate(XX_data)

## UPLOADING ADDITIONAL DATA ##
tdUpload(XX_data, 'XX_data')

## ORGANIZE DATA ##
td("create table xxxx as (...)")

## DEBUGGING ##
tdRows("xxxx")
tdNames("xxxx")
tdHead("xxxx")
tdShow("xxxx")
tdCpu()
tdDisk()
tdSpool()

## EXPORT TO R ##
ourData = td("sel * from xxxx")

## ANALYSES ##
## e.g. Regression, plots, summaries, etc.
plot(y ~ x, data=ourData)

## DISCONNECT ##
tdClose()
```

Alternatively, you can write a SQL script and submit it with R using the following commands. Please note that in this approach, each command has to end with a semicolon. If a semicolon is needed in the code outside of this purpose, please precede it with a forward slash (/), e.g. where column_name = "/;"
=======

```
library(tdR)
tdFile(<file_name.sql>)
```


