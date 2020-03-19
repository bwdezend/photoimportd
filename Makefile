build:
	go build -o photoimport *go 

lint:
	find . -type f -name "*go" -exec gofmt -w {} \;

run:
	go run *go
