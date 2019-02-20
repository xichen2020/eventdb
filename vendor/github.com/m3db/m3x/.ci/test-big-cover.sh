#!/bin/bash
set -e

source "$(dirname $0)/variables.sh"

TARGET=${1:-cover.out}
EXCLUDE_FILE=${2:-.excludecoverage}
LOG=${3:-test.log}

rm $TARGET &>/dev/null || true
echo "mode: count" > $TARGET
echo "" > $LOG

DIRS=""
for DIR in $SRC;
do
  if ls $DIR/*_test.go &> /dev/null; then
    DIRS="$DIRS $DIR"
  fi
done

PROFILE_REG="profile_reg.tmp"
PROFILE_BIG="profile_big.tmp"
TEST_EXIT=0

# run big tests one by one
TEST_FLAGS="-v -timeout 5m -covermode count"
echo "test-cover begin: concurrency 1, +big"
for DIR in $DIRS; do
  if cat $DIR/*_test.go | grep "// +build" | grep "big" &>/dev/null; then
    # extract only the tests marked "big"
    BIG_TESTS=$(cat <(go test $DIR -tags big -list '.*' | grep -v '^ok' | grep -v 'no test files' ) \
                    <(go test $DIR -list '.*' | grep -v '^ok' | grep -v 'no test files')            \
                    | sort | uniq -u | paste -sd'|' -)
    go test $TEST_FLAGS -tags big -run $BIG_TESTS -coverprofile $PROFILE_BIG \
      -coverpkg $(go list ./... | grep -v /vendor/ | paste -sd, -)           \
      $DIR | tee $LOG
    BIG_TEST_EXIT=${PIPESTATUS[0]}
    # Only set TEST_EXIT if its already zero to be prevent overwriting non-zero exit codes
    if [ "$TEST_EXIT" = "0" ]; then
      TEST_EXIT=$BIG_TEST_EXIT
    fi
    if [ "$BIG_TEST_EXIT" != "0" ]; then
      continue
    fi
    if [ -s $PROFILE_BIG ]; then
      cat $PROFILE_BIG | tail -n +2 >> $PROFILE_REG
    fi
  fi
done

filter_cover_profile $PROFILE_REG $TARGET $EXCLUDE_FILE

find . -not -path '*/vendor/*' | grep \\.tmp$ | xargs -I{} rm {}
echo "test-cover result: $TEST_EXIT"

exit $TEST_EXIT
