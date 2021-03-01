
build: cmd/cmd_musql.go
	go build -ldflags "-s -w" -o musql $<
