#!/bin/bash

echo 'time,tx,msg,path,args,res,err'
jq -s --raw-output '.[] | [.time, .tx, .msg, .path, .args, .res, .err] | @csv'
