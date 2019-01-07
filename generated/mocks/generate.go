package mocks

// mockgen rules for generating mocks for exported interfaces (reflection mode).
//go:generate sh -c "mockgen -package=digest $PACKAGE/digest FdWithDigestWriter | genclean -pkg $PACKAGE/digest -out $GOPATH/src/$PACKAGE/digest/writer_mock.go"
//go:generate sh -c "mockgen -package=index $PACKAGE/index DocIDSetIterator | genclean -pkg $PACKAGE/index -out $GOPATH/src/$PACKAGE/index/document_mock.go"
//go:generate sh -c "mockgen -package=iterator $PACKAGE/values/iterator ForwardBoolIterator,ForwardIntIterator,ForwardDoubleIterator,ForwardStringIterator,ForwardTimeIterator | genclean -pkg $PACKAGE/values/iterator -out $GOPATH/src/$PACKAGE/values/iterator/iterator_mock.go"
//go:generate sh -c "mockgen -package=values $PACKAGE/values BoolValues,IntValues,DoubleValues,StringValues,TimeValues | genclean -pkg $PACKAGE/values -out $GOPATH/src/$PACKAGE/values/values_mock.go"

// mockgen rules for generating mocks for unexported interfaces (file mode).
//go:generate sh -c "mockgen -package=storage -destination=$GOPATH/src/$PACKAGE/storage/segment_base_mock.go -source=$GOPATH/src/$PACKAGE/storage/segment_base.go"
