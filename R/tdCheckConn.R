# TODO: Checks for Teradata connection
# 
# Author: Linh Tran
# Date: Mar 10, 2016
# Email: Linh_m_tran@apple.com
###############################################################################


tdCheckConn = function(...) {

	args = list(...)[[1]]
	argNames = names(args)
	
	if ("conn" %in% argNames) {
		conn = args$conn
		if(is.null(attr(conn, "tmpConnection"))) attr(conn, "tmpConnection") = FALSE
		
	} else if ("username" %in% argNames & "password" %in% argNames) {
		if ('classPath' %in% argNames) {
			classPath=args$classPath
		} else classPath=NULL
		if ('addr' %in% argNames) {
			addr = args$addr
			if ("db" %in% argNames) {
				conn = with(args, tdConn(username=username, password=password, addr=addr, db=db, classPath=classPath))
			} else conn = with(args, tdConn(username=username, password=password, addr=addr, classPath=classPath))
		} else {
			if ("db" %in% argNames) {
				conn = with(args, tdConn(username=username, password=password, db=db, classPath=classPath))
			} else conn = with(args, tdConn(username=username, password=password, classPath=classPath))
		}
		attr(conn, "tmpConnection") = TRUE
		
	} else {
		conn = try(get("conn", envir=.GlobalEnv), TRUE)
		if(is.null(attr(conn, "tmpConnection"))) attr(conn, "tmpConnection") = FALSE
		if(inherits(conn, "try-error")) stop("No connection or connection profile detected.") 
	}
	
	return(conn)
	
}
