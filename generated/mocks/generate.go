package mocks

// mockgen rules for generating mocks for exported interfaces (reflection mode).
//go:generate sh -c "mockgen -package=int $PACKAGE/encoding/int Encoder,Iterator,Decoder | genclean -pkg $PACKAGE/encoding/int -out $GOPATH/src/$PACKAGE/encoding/int/encoding_mock.go"

// mockgen rules for generating mocks for unexported interfaces (file mode).
//go:generate sh -c "mockgen -package=storage -destination=$GOPATH/src/$PACKAGE/storage/segment_mock.go -source=$GOPATH/src/$PACKAGE/storage/segment.go"
