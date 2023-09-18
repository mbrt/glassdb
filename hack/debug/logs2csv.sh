#!/bin/bash

echo 'tx,msg,path,args,res,err'
jq -s --raw-output '.[] | [.tx, .msg, .path, .args, .res, .err] | @csv'
