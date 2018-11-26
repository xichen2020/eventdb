package mocks

//go:generate sh -c "mockgen -package=int $PACKAGE/encoding/int Encoder,Iterator,Decoder | genclean -pkg $PACKAGE/encoding/int -out $GOPATH/src/$PACKAGE/encoding/int/encoding_mock.go"
