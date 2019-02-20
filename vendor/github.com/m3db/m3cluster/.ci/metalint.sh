#!/bin/bash

if [[ $# -ne 2 ]] && [[ $# -ne 3 ]]; then
  echo "Usage: $0 <metalinter-config-file> <exclude-file> [<lint-dir>]"
  exit 1
fi

config_file=$1
exclude_file=$2
lint_dir=${3:-.}

if [[ ! -f $exclude_file ]]; then
  echo "exclude-file ($exclude_file) does not exist"
  exit 1
fi

LINT_OUT=$(gometalinter --tests --config $config_file --vendor $lint_dir/... | egrep -v -f $exclude_file)
if [[ $LINT_OUT == "" ]]; then
	echo "Metalinted succesfully!"
	exit 0
fi

echo "$LINT_OUT"
if [[ $LINT_OUT == *"maligned"* ]]; then
	echo "If you received an error about struct size, try re-ordering the fields in descending order by size."
  echo "https://github.com/dominikh/go-tools/tree/master/cmd/structlayout"
  echo "http://golang-sizeof.tips"
fi
exit 1