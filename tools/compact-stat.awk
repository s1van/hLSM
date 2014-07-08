function getms(string) {
		hh = substr(string, 12, 2);
		mm = substr(string, 15, 2);
		ss = substr(string, 18, 2);
		ms = substr(string, 21, 3);

		time = ((hh*60 + mm) * 60 + ss) * 1000 + ms;
		return time;
}

BEGIN{ORS="\t"; start=-1; end=-1; minor_start=-1; minor_end=-1;
	major_c=0; minor_c=0; major_mv_c=0; mv_c=0
	major_byte=0; minor_byte=0; major_mv_byte=0; mv_byte=0;
	major_fnum=0; gap=0;
	work=0; minor_work=0; major_mv_work=0; }
{
	if (match("Compacting", $3)>0) {
		start = getms($1); 

		if (end != -1) {
			gap = start - end + gap;
		}

	} else if (match("Level-0", $3)>0 && match("started", $6)>0) {
		minor_start=getms($1);

	} else if (match("Compacted", $3)>0){
		end = getms($1);

		match($4,"[0-9]+", ret);
		ulevel_fnum = ret[0];
		match($6,"[0-9]+", ret);
		blevel_fnum = ret[0];

		if (blevel_fnum > 0) {
			work = work + end - start;
			major_byte = major_byte + $9;
			major_fnum=major_fnum + ulevel_fnum + blevel_fnum;
			major_c = major_c + 1;
		} else {
			major_mv_work = major_mv_work + end - start;
			major_mv_byte = major_mv_byte + $9;
			major_mv_c = major_mv_c + 1;
		}
		
		start = -1;

	} else if (match("Level-0", $3)>0) {
		minor_end = getms($1);
		minor_work = minor_work + minor_end - minor_start;
		minor_byte = minor_byte + $6;
		
		minor_start = -1;
		minor_c = minor_c + 1;

	} else if (match("Moved", $3)>0) {
		mv_c = +1;
		mv_byte = +$7;
	}
}
END{work = work/1000; minor_work=minor_work/1000; major_mv_work=major_mv_work/1000; gap=gap/1000;
	major_mb = major_byte/1048576; minor_mb=minor_byte/1048576; 
	major_mv_mb=(mv_byte + major_mv_byte)/1048576;
if(title) printf("%s\t%s\t%s\t%s\t%s\n",
		"#major #minor #mv #file",
		"major.s mionr.s mv.s",
		"thoughput file.s.avg",
		"gap major.t minor.t mv.t",
		"major.t.avg major.s.avg");
print major_c, minor_c, major_mv_c + mv_c, major_fnum, "\t",
	major_mb, minor_mb, major_mv_mb, "\t",
	(2*major_mb+2*minor_mb)/(work+minor_work), major_mb/major_fnum,"\t",
	gap, work, minor_work, major_mv_work;

printf("\t%.3f %.3f\n", work/major_c, major_mb/major_c) 
}
