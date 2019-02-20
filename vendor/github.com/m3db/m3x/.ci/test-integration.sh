#!/bin/bash
set -e

source "$(dirname $0)/variables.sh"

COVERFILE=${1:-profile.cov}
EXCLUDE_FILE=${2}
TAGS="integration"
DIR="integration"
INTEGRATION_TIMEOUT=${INTEGRATION_TIMEOUT:-10m}
COVERMODE=count
SCRATCH_FILE=${COVERFILE}.tmp

echo "mode: ${COVERMODE}" > $SCRATCH_FILE

# go1.10 has an open bug for coverage reports that requires a *terrible* hack
# to workaround. See https://github.com/golang/go/issues/23883 for more details
GO_MINOR_VERSION=$(go version | awk '{print $3}' | cut -d '.' -f 2)
if [ ${GO_MINOR_VERSION} -ge 10 ]; # i.e. we're on go1.10 and up
then
  echo "Generating dummy integration file with all the packages listed for coverage"
  DUMMY_FILE_PATH=./integration/coverage_imports.go
  if [ -f ${DUMMY_FILE_PATH} ]; then
    rm -f ${DUMMY_FILE_PATH} # delete file if it exists (only happens when running on a laptop)
  fi
  # NB: need to do this in two steps or the go compiler compiles the partial file and is :(
  generate_dummy_coverage_file integration integration > coverage_imports_file.out
  mv coverage_imports_file.out ${DUMMY_FILE_PATH}
fi

# compile the integration test binary
go test -test.c -test.tags=${TAGS} -test.covermode ${COVERMODE} \
  -test.coverpkg $(go list ./... |  grep -v /vendor/ | paste -sd, -) ./${DIR}

# list the tests
TESTS=$(./integration.test -test.v -test.short | grep RUN | tr -s " " | cut -d ' ' -f 3)
# can use the version below once the minimum version we use is go1.9
# TESTS=$(./integration.test -test.list '.*')

# execute tests one by one for isolation
for TEST in $TESTS; do
  ./integration.test -test.v -test.run $TEST -test.coverprofile temp_${COVERFILE} \
  -test.timeout $INTEGRATION_TIMEOUT ./integration
  TEST_EXIT=$?
  if [ "$TEST_EXIT" != "0" ]; then
    echo "$TEST failed"
    exit $TEST_EXIT
  fi
  cat temp_${COVERFILE} | grep -v "mode:" >> ${SCRATCH_FILE}
  sleep 0.1
done

filter_cover_profile $SCRATCH_FILE $COVERFILE $EXCLUDE_FILE

echo "PASS all integrations tests"
