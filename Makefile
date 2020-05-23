.PHONY: test

build: check
	go build -o photoimportd *go 


check: go_fmt go_lint go_vet

go_fmt:
	docker run \
		--rm \
		-v $(PWD):/installer \
		-w /installer \
		golang \
		bash -c "find . -path ./vendor -prune -o -name '*.go' -exec gofmt -l {} \; | tee fmt.out && if [ -s fmt.out ] ; then exit 1; fi "


go_vet:
	docker run\
		--rm \
		-v $(PWD):/installer \
		-w /installer \
		golang \
		bash -c "go vet ./..."

go_lint:
	docker run \
		--rm \
		-v $(PWD):/installer \
		-w /installer \
		golang \
		bash -c 'go get golang.org/x/lint/golint && go list ./... | xargs -L1 golint -set_exit_status'


debug:
	go run *go -debug -sleep 30

run:
	go run *go 


install:
	cp photoimportd /usr/local/bin/photoimportd 
	chmod +x /usr/local/bin/photoimportd

deploy: build install

test:
	find test -name "test.db" -type f -delete
	find test/in -type f -delete
	find test/out -type f -delete
	go run *go -metrics -debug -db test/test.db -dst test/out -src test/in -sleep 10

clean:
	find . -type d -name "*string*" -exec rm -rf {} \;
	rm photoimportd
	rm -rf test/test.db
	rm -rf test/in
	rm -rf test/out
