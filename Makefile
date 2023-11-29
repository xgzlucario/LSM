gen-proto:
	rm -rf pb && protoc --go_out=. --go_opt=Mlsm.proto=pb/ lsm.proto