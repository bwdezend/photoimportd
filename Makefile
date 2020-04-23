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


clean:
	find . -type d -name "*string*" -exec rm -rf {} \;
	rm photoimportd
