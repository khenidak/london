mkfile_path:=$(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dirpath:=$(shell cd $(shell dirname $(mkfile_path)); pwd)

binaryName:=london
outputDir:=_output
hackDir:=hack
testingVarsFileName:="testing_vars"
varFilePath:=$(mkfile_dirpath)/$(hackDir)/$(testingVarsFileName)
kubernetes_testdir_name="kubernetes_src"
kubernetes_ver="v1.20.0"

## version mgmt
VERSION := $(shell git rev-parse --short HEAD)
BUILDTIME := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

GOLDFLAGS += -X main.Version=$(VERSION)
GOLDFLAGS += -X main.Buildtime=$(BUILDTIME)
GOFLAGS = -ldflags "$(GOLDFLAGS)"

help: ## show this help.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

clean: ## cleans output
	@rm -r $(mkfile_dirpath)/$(outputDir) || exit 0

prep-outputdir: ## creates output directory
	@mkdir -p $(mkfile_dirpath)/$(outputDir)

#Depend on get-kubernetes?
build-binary: prep-outputdir ## builds binary and drop it  in output directory
	@echo "** building binary with version:$(VERSION)"
	@go build -o $(mkfile_dirpath)/$(outputDir)/$(binaryName) $(GOFLAGS) $(mkfile_dirpath)/.
	@echo "** built binary is at:$(mkfile_dirpath)/$(outputDir)/$(binaryName) "

get-kubernetes: ## gets kubernetes source code for e2e tests
	@$(mkfile_dirpath)/$(hackDir)/get_kubernetes.sh "$(kubernetes_ver)" "$(mkfile_dirpath)/$(kubernetes_testdir_name)" $(mkfile_dirpath)


e2e-test: get-kubernetes  ## runs e2e tests against an in-proc kube-api-server
	@echo "** running e2e test @ $(mkfile_dirpath)/test/e2e"
	@echo "****************************************************************"
	@echo "* The following tests runs etcd api and api-server in proc"
	@echo "* expect long running tests..."
	@echo "****************************************************************"
	@LONDON_TESTING_VARS=$(varFilePath) KUBE_PANIC_WATCH_DECODE_ERROR=true go test $(mkfile_dirpath)/test/e2e -count=1 $(ADD_TEST_ARGS)

integration-tests: get-kubernetes ## runs integeration test
	@echo "** running integration test @ $(mkfile_dirpath)/test/integration"
	@LONDON_TESTING_VARS=$(varFilePath) go test $(mkfile_dirpath)/test/integration  -count=1 $(ADD_TEST_ARGS)

unit-tests: get-kubernetes ## runs unit test
	@echo "** running unit test in @ $(mkfile_dirpath)/pkg/backend/revision"
	@LONDON_TESTING_VARS=$(varFilePath) go test $(mkfile_dirpath)/pkg/backend/storerecord   -count=1 $(ADD_TEST_ARGS)  || exit 1
	@echo "** running unit test in @ $(mkfile_dirpath)/pkg/backend/revision"
	@LONDON_TESTING_VARS=$(varFilePath) go test $(mkfile_dirpath)/pkg/backend/revision  -count=1 $(ADD_TEST_ARGS) || exit 1
	@echo "** running unit test in @ $(mkfile_dirpath)/pkg/backend/filter"
	@LONDON_TESTING_VARS=$(varFilePath) go test $(mkfile_dirpath)/pkg/backend/filter   -count=1 $(ADD_TEST_ARGS) || exit 1
	@echo "** running unit test in @ $(mkfile_dirpath)/pkg/backend"
	@LONDON_TESTING_VARS=$(varFilePath) go test $(mkfile_dirpath)/pkg/backend  -count=1  $(ADD_TEST_ARGS) || exit 1

test: unit-tests integration-tests e2e-test ## runs all tests
