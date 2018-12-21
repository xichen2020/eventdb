package mocks

// mockgen rules for generating mocks for exported interfaces (reflection mode).
//go:generate sh -c "mockgen -package=digest $PACKAGE/digest FdWithDigestWriter | genclean -pkg $PACKAGE/digest -out $GOPATH/src/$PACKAGE/digest/writer_mock.go"
//go:generate sh -c "mockgen -package=encoding $PACKAGE/encoding RewindableIntIterator,RewindableStringIterator,ForwardDoubleIterator,ForwardBoolIterator,RewindableTimeIterator | genclean -pkg $PACKAGE/encoding -out $GOPATH/src/$PACKAGE/encoding/encoding_mock.go"

// mockgen rules for generating mocks for unexported interfaces (file mode).
//go:generate sh -c "mockgen -package=storage -destination=$GOPATH/src/$PACKAGE/storage/segment_mock.go -source=$GOPATH/src/$PACKAGE/storage/segment.go"
