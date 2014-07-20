library(gplots)
library(ggplot2)

###############
# Example:  
# paint("micro_tp",draw_grouped_bar,list("../final/tp",c("R/W=1/2", "R/W=1","R/W=2","R/W=4","R/W=8"), c("LevelDB-RAID0", "LevelDB-SSD", "hLSM", "hLSM-SSD"), ylab="Transaction Rate(ops/sec)", xlab="Read/Write Ratio"), 8,6)
###############
# draw_cols("f2_8_32.tp", c("2MB","8MB","32MB"), c("1/8","1/4","1/2","1","2","4"), "R/W Ratio", "Transaction Rate (#queries/second)")
###############

paint = function(out, artist, pens, w=5,h=10) {

	fs=2.6
	setEPS()
	postscript(paste(out,".eps", sep=""), width=w, height=h)
	par(cex=1.3, font=fs, font.lab=fs, font.axis=fs)
	do.call(artist, pens)
	dev.off()

	pdf(paste(out,".pdf", sep=""), width=w, height=h)
	par(cex=1.3, font=fs, font.lab=fs, font.axis=fs)
	do.call(artist, pens)
	dev.off()
}

draw_cols_ = function(data, colname, xticks, xlabel, ylabel) {
	names(data) <- colname
	colnum <- length(colname)

	xat <- 1:length(xticks)
	# get the range for the x and y axis 
	xrange <- range(xat) 
	yrange <- range(data) 

	# set up the plot 
	plot(xrange, yrange, type="n", xlab=xlabel, ylab=ylabel, xaxt='n') 
	axis(side = 1, at = xat, labels = xticks)
	colors <- gray.colors(colnum) 
	linetype <- c(1:colnum) 
	plotchar <- seq(18,18+colnum,1)

	# add grid
	grid(lwd=par("lwd")*2)

	# add lines 
	for (i in 1:colnum) { 
	  col <- data[colname[i]]
	  lines(cbind(xat, col), type="b", lwd=1.5,
	    lty=linetype[i], col=colors[i], pch=plotchar[i]) 
	} 

	# add a legend 
	smartlegend(x="right",y="top", inset=0, cex=0.8,
        colname, col=colors, pch=plotchar, lty=linetype, font=1)
}

draw_cols = function(file, colname, xticks, xlabel, ylabel) {
	data <- read.table(file)
	draw_cols_(data, colname, xticks, xlabel, ylabel)
}

draw_time_series = function(t, ys, ylabel="", hasXTick='s', xlabel="", ytop=Inf){
	colnum <- length(ys)
	
	xat <- 1:length(t)
    # get the range for the x and y axis
    xrange <- range(t)
    yrange <- range(ys)
	yrange[2] <- min(yrange[2], ytop)

	# set up the plot 
	if (xlabel=="") {bm<-1; tm<-1} else {bm<-4; tm<-1;}
	par(mar=c(bm, 4, tm, 0))  # c(bottom, left, top, right)

	plot(xrange, yrange, type="n", xlab=xlabel, ylab=ylabel, xaxt=hasXTick, frame.plot=F) 
	colors <- gray.colors(colnum, start=0, end=0.3) 
	linetype <- c(1:colnum) 

	# add grid
	grid(lwd=par("lwd")*1.2)

	# add lines 
	for (i in 1:colnum) { 
	  col <- ys[i]
	  lines(cbind(t, col), type="l", lwd=1.5,
	    lty=linetype[i], col=colors[i]) 
	} 
}

# first column: id, second column: #appearance
draw_hist_ = function(x, xlabel, ylabel, title) {
	h <- hist(x, xlab=xlabel, ylab=ylabel, col="grey", breaks=max(x), main=title)
	xfit<-seq(min(x),max(x),length=40) 
	yfit<-dnorm(xfit,mean=mean(x),sd=sd(x)) 
	yfit <- yfit*diff(h$mids[1:2])*length(x) 
	lines(xfit, yfit, col="black", lwd=2)
}

######################### End of Basic Functions ###########################

draw_hist = function(file, xlabel, ylabel, title) {
	data <- read.table(file)
    hn <- data[,1]
	draw_hist_(hn, xlabel, ylabel, title)
}


draw_grouped_bar = function(file, xticks, legs, xlabel="", ylabel="", inverse=TRUE) {
	if (inverse)
		data <- t(read.table(file))
	else
		data <- read.table(file)

	barplot(as.matrix(data/1000), xlab=xlabel, ylab=ylabel, density=100/dim(data)[1]*(1:dim(data)[1]),
		legend = legs, args.legend=list(x="topright"), beside=TRUE, names.arg=xticks)
}


plot_one_ts = function(file, ylabel) {
	data <- read.table(file)
	t <- data[1]
	ys <- data[-1]
	
	draw_time_series(t, ys, ylabel)
}


plot_TL = function(file, leg, xlab) {
	par(mfrow=c(2,1)) 
	ys <- read.table(file)
	t <- 2 * 2:(length(ys[[1]])+1)
	n <- length(ys[1,])

	#tp_pos <- seq(2,n,2)
	#lc_pos <- seq(1,n,2)
	lc_pos <- 1:n/2
	tp_pos <- (n/2+1) :n
	draw_time_series(t, ys[tp_pos], "Throughput (ops/sec)")
	draw_time_series(t, ys[lc_pos], "Latency (ms)", xlabel = xlab)

	lnum = n/2
	colors <- gray.colors(lnum, start=0, end=0.3) 
	linetype <- c(1:lnum) 
	smartlegend(x="right",y="top", inset=0, cex=0.7, text.font=1,
        leg, col=colors, lty=linetype)
}

plot_TL_m = function(files, tags) {
	fnum <- length(files)
	par(mfrow=c(2,fnum), oma = c(2, 0, 0, 0))

	ylab <- "Throughput (ops/sec)"
	for (i in 1:fnum) {
		ys <- read.table(files[i])
		t <- 2 * 2:(length(ys[[1]])+1)
		draw_time_series(t, ys[3:4], ylab, hasXTick='n')
		ylab <- ""
	}

	colors <- gray.colors(2) 
	linetype <- c(1:2) 
	smartlegend(x="right",y="top", inset=0, cex=0.8,
        c("Read", "Write"), col=colors, lty=linetype)

	ylab <- "Latency (ms)"
	for (i in 1:fnum) {
		ys <- read.table(files[i])/1000
		t <- 2 * 2:(length(ys[[1]])+1)
		draw_time_series(t, ys[1:2], ylab, xlabel=tags[i], ytop=15)
		ylab <- ""
	}

	mtext("Elapsed Time(s)", outer = TRUE, cex = 0.7, side=1)
}

plot_3L_m = function(files, tags) {
	fnum <- length(files)
	par(mfrow=c(2,fnum), oma = c(2, 0, 0, 0))

	ylab <- "Scan Latency (ms)"
	for (i in 1:fnum) {
		ys <- read.table(files[i])/1000
		t <- 2 * 2:(length(ys[[1]])+1)
		draw_time_series(t, ys[3], ylab, hasXTick='n', ytop=50)
		ylab <- ""
	}


	ylab <- "R/W Latency (ms)"
	for (i in 1:fnum) {
		ys <- read.table(files[i])/1000
		t <- 2 * 2:(length(ys[[1]])+1)
		draw_time_series(t, ys[1:2], ylab, xlabel=tags[i], ytop=10)
		ylab <- ""
	}

	colors <- gray.colors(2) 
	linetype <- c(1:2) 
	smartlegend(x="right",y="top", inset=0, cex=0.8,
        c("Read", "Insert"), col=colors, lty=linetype)

	mtext("Elapsed Time(s)", outer = TRUE, cex = 1, side=1)
}

