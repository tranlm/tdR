# tdR R-package

This is the current version of the tdR R package (version 0.9.\*).

## How to install from Gitlab ##

To install the tdR package, we recommend cloning the gitlab repository
=======

### Gitlab
With the terminal open, you can install the cdmApple package using the following script.

```shell
git clone git@gitlab.sd.apple.com:cdmApple/tdR.git
R CMD INSTALL tdR
```

### Download
Alternatively, the package is available for download. Safari unzips the compressed folder, and will usually add a suffix to the name. The package can then be install with the command

```shell
R CMD INSTALL tdR-master-XXXX
```

where XXXX is the suffix added to the end of the uncompressed folder.


An example of the typical R script workflow
=======
```
library(tdR)

## ESTABLISH CONNECTION ##
## nb. Do not change the name if you want subsequent code to discover the connection automatically
conn = tdConn()

## ORGANIZE DATA ##
td("create table xxxx as (...)")

## DEBUGGING ##
tdDim("xxxx")
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


