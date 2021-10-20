#!/bin/bash
make clean &> /dev/null 2>&1
make &> /dev/null 2>&1

for a in {1..100}
do
  echo "starting "$a
  sudo ./stop.sh
  sudo ./start.sh
  sudo ./grade.sh

done
