package mocks

// mockgen rules for generating mocks for exported interfaces (reflection mode).
//go:generate sh -c "mockgen -package=digest $PACKAGE/digest FdWithDigestWriter | genclean -pkg $PACKAGE/digest -out $GOPATH/src/$PACKAGE/digest/digest_mock.go"
//go:generate sh -c "mockgen -package=index $PACKAGE/index DocIDSet,DocIDSetIterator,SeekableDocIDSetIterator,DocIDPositionIterator | genclean -pkg $PACKAGE/index -out $GOPATH/src/$PACKAGE/index/index_mock.go"
//go:generate sh -c "mockgen -package=field -destination=$GOPATH/src/$PACKAGE/index/field/field_mock.go $PACKAGE/index/field BaseFieldIterator,CloseableNullField,CloseableBoolField,CloseableIntField,CloseableDoubleField,CloseableBytesField,CloseableTimeField,DocsField"
//go:generate sh -c "mockgen -package=iterator $PACKAGE/values/iterator ForwardBoolIterator,ForwardIntIterator,ForwardDoubleIterator,ForwardBytesIterator,ForwardTimeIterator,SeekableBoolIterator,SeekableIntIterator,SeekableDoubleIterator,SeekableBytesIterator,SeekableTimeIterator,PositionIterator | genclean -pkg $PACKAGE/values/iterator -out $GOPATH/src/$PACKAGE/values/iterator/iterator_mock.go"
//go:generate sh -c "mockgen -package=values $PACKAGE/values BoolValues,IntValues,DoubleValues,BytesValues,TimeValues | genclean -pkg $PACKAGE/values -out $GOPATH/src/$PACKAGE/values/values_mock.go"
