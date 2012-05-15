
default:
	go build

fmt:
	gofmt -w *.go

test:
	go test
