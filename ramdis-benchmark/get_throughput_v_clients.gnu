###
### WARNING: Requires gnuplot 4.4
###

set terminal svg size 800, 600 fname 'Verdana' fsize 12
set output outputFile

set title "GET Throughput"

set style line 1 lt 1 lw 4 pt 1 linecolor rgb "#E41A1C"
set style line 2 lt 2 lw 4 pt 2 linecolor rgb "#377EB8"
set style line 3 lt 3 lw 4 pt 3 linecolor rgb "#4DAF4A"
set style line 4 lt 4 lw 4 pt 4 linecolor rgb "#984EA3"
set style line 5 lt 5 lw 4 pt 5 linecolor rgb "#FF7F00"
set style line 6 lt 6 lw 4 pt 6 linecolor rgb "#FFFF33"
set style line 7 lt 7 lw 4 pt 7 linecolor rgb "#A65628"
set style line 8 lt 8 lw 4 pt 8 linecolor rgb "#F781BF"

set key top left
set grid

set xlabel "Clients"
#set logscale x 2
#set xrange [128:1048576]
#set xrange [128:4096]

set ylabel "Operations per second"
set yrange [0:1200000]
#set ytics 1
#set logscale y

plot inputFile using 1:2 with linespoints ls 5 title "Throughput"
