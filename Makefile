gen-proto:
	rm -rf pb && protoc --go_out=. --go_opt=Mlsm.proto=pb/ lsm.proto

test-cover:
	go test -race \
	-coverpkg=./... ./memdb ./refmap \
	-coverprofile=cover.txt -covermode=atomic
	go tool cover -html=cover.txt -o coverage.html

clear:
	rm -f *.sst
	rm -f coverage.*

run:
	rm -rf lsm
	go run example/main.go