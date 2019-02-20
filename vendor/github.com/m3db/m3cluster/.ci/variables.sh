#!/bin/bash
# set PACKAGE in .travis.yml
export VENDOR_PATH=$PACKAGE/vendor
export LICENSE_BIN=$GOPATH/src/$PACKAGE/.ci/uber-licence/bin/licence
export GO15VENDOREXPERIMENT=1

FIND_ROOT="./"
if [ "$SRC_ROOT" != "" ]; then
  FIND_ROOT=$SRC_ROOT
fi

find_dirs() {
  find $FIND_ROOT -maxdepth 10 -not -path '*/.git*' -not -path '*/.ci*' -not -path '*/_*' -not -path '*/vendor/*' -type d
}

BASE_SRC=$(find_dirs)
if [ "$SRC_EXCLUDE" != "" ]; then
  BASE_SRC=$(find_dirs | grep -v $SRC_EXCLUDE)
fi
export SRC=$BASE_SRC

filter_cover_profile() {
  local input_profile_file=$1
  local output_file=$2
  local exclude_file=$3
  if [ -z $input_profile_file ] ; then
    echo 'input_profile_file (i.e. $1) is not set'
    exit 1
  fi
  if [ -z $output_file ] ; then
    echo 'output_file (i.e. $2) is not set'
    exit 1
  fi
  if [ ! -z $exclude_file ] && [ -f $exclude_file ] ; then
    cat $input_profile_file | egrep -v -f $exclude_file | grep -v 'mode:' >> $output_file
  else
    cat $input_profile_file | grep -v "_mock.go" | grep -v 'mode:' >> $output_file
  fi
}

export -f filter_cover_profile

# go1.10 has an open bug for coverage reports that requires a *terrible* hack
# to workaround. See https://github.com/golang/go/issues/23883 for more details
function generate_dummy_coverage_file() {
  local package_name=$1
  local build_tag=$2
go list ./$FIND_ROOT/... | grep -v vendor | grep -v "\/main$" | grep -v "\/${package_name}" > repo_packages.out
INPUT_FILE=./repo_packages.out python <<END
import os
input_file_path = os.environ['INPUT_FILE']
input_file = open(input_file_path)
print '// +build ${build_tag}'
print
print 'package ${package_name}'
print
print 'import ('
for line in input_file.readlines():
    line = line.strip()
    print '\t _ "%s"' % line
print ')'
print
END
}

export -f generate_dummy_coverage_file
