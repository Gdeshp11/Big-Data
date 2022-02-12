#!/bin/bash
#java stockMarket

rm -rf in/* out/
hadoop jar stockmarket.jar stockMarket in out $1
