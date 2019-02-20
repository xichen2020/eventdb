#!/bin/bash
set -e

source "$(dirname $0)/variables.sh"

TARGET=${1:-profile.cov}
EXCLUDE_FILE=${2:-.excludecoverage}
LOG=${3:-test.log}
CI_DIR=${4:-.}

rm $TARGET &>/dev/null || true
echo "mode: atomic" > $TARGET
echo "" > $LOG

DIRS=""
for DIR in $SRC;
do
  if ls $DIR/*_test.go &> /dev/null; then
    DIRS="$DIRS $DIR"
  fi
done

if [ "$NPROC" = "" ]; then
  NPROC=$(getconf _NPROCESSORS_ONLN)
fi

echo "test-cover begin: concurrency $NPROC"

PROFILE_REG="profile_reg.tmp"

TEST_FLAGS="-v -race -timeout 5m -covermode atomic"
go run $CI_DIR/.ci/gotestcover/gotestcover.go $TEST_FLAGS -coverprofile $PROFILE_REG -parallelpackages $NPROC $DIRS | tee $LOG
TEST_EXIT=${PIPESTATUS[0]}

filter_cover_profile $PROFILE_REG $TARGET $EXCLUDE_FILE

find . -not -path '*/vendor/*' | grep \\.tmp$ | xargs -I{} rm {}
echo "test-cover result: $TEST_EXIT"

exit $TEST_EXIT
