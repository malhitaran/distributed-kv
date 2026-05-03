#!/bin/bash

# Extract data from vegeta results
cat results-10k.bin | vegeta encode | \
    jq -r '[.latency/1000000, .code] | @csv' > latencies.csv

# Create gnuplot script
cat > plot.gp <<EOF
set terminal pngcairo size 1200,800 font 'Arial,12'
set output 'latency-distribution.png'
set title 'Latency Distribution (10K req/s)'
set xlabel 'Request Number'
set ylabel 'Latency (ms)'
set grid
set datafile separator ','
plot 'latencies.csv' using 1 with lines title 'Latency' lc rgb 'blue'
EOF

gnuplot plot.gp

echo "Generated latency-distribution.png"