gen-proto:
	rm -rf pb && protoc --go_out=. --go_opt=Mlsm.proto=pb/ lsm.proto

test-cover:
	go test -race -coverprofile=coverage.txt -covermode=atomic
	go tool cover -html=coverage.txt -o coverage.html