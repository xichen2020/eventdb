SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
include $(SELF_DIR)/.ci/common.mk

SHELL=/bin/bash -o pipefail

html_report           := coverage.html
test                  := .ci/test-cover.sh
test_ci_integration   := .ci/test-integration.sh
convert-test-data     := .ci/convert-test-data.sh
coverfile             := cover.out
coverage_xml          := coverage.xml
junit_xml             := junit.xml
coverage_exclude      := .excludecoverage
test_log              := test.log
metalint_check        := .ci/metalint.sh
metalint_config       := .metalinter.json
metalint_exclude      := .excludemetalint
package_root          := github.com/xichen2020/eventdb
gopath_prefix         := $(GOPATH)/src
vendor_prefix         := vendor
generics_output_dir   := generated/generics
generics_rules_dir    := generated/generics
auto_gen              := .ci/auto-gen.sh
license_dir           := .ci/uber-licence
license_node_modules  := $(license_dir)/node_modules

BUILD           := $(abspath ./bin)
LINUX_AMD64_ENV := GOOS=linux GOARCH=amd64 CGO_ENABLED=0

SERVICES := \
	eventdb

.PHONY: setup
setup:
	mkdir -p $(BUILD)

define SERVICE_RULES

.PHONY: $(SERVICE)
$(SERVICE): setup
	@echo Building $(SERVICE)
	$(VENDOR_ENV) go build -o $(BUILD)/$(SERVICE) ./services/$(SERVICE)/main/.

.PHONY: $(SERVICE)-linux-amd64
$(SERVICE)-linux-amd64:
	$(LINUX_AMD64_ENV) make $(SERVICE)

endef

.PHONY: services
services: $(SERVICES)

.PHONY: services-linux-amd64
services-linux-amd64:
	$(LINUX_AMD64_ENV) make services

$(foreach SERVICE,$(SERVICES),$(eval $(SERVICE_RULES)))

.PHONY: metalint
metalint: install-metalinter install-linter-badtime
	@($(metalint_check) $(metalint_config) $(metalint_exclude) && echo "metalinted successfully!") || (echo "metalinter failed" && exit 1)

.PHONY: test-internal
test-internal:
	@which go-junit-report > /dev/null || go get -u github.com/sectioneight/go-junit-report
	($(test) $(coverfile) && cat $(coverfile) | egrep -v -f $(coverage_exclude) > $(coverfile).tmp && mv $(coverfile).tmp $(coverfile)) | tee $(test_log)

.PHONY: test-integration
test-integration:
	go test -v -tags=integration ./integration

.PHONY: test-xml
test-xml: test-internal
	go-junit-report < $(test_log) > $(junit_xml)
	gocov convert $(coverfile) | gocov-xml > $(coverage_xml)
	@$(convert-test-data) $(coverage_xml)
	@rm $(coverfile) &> /dev/null

.PHONY: test
test: test-internal
	gocov convert $(coverfile) | gocov report

.PHONY: testhtml
testhtml: test-internal
	gocov convert $(coverfile) | gocov-html > $(html_report) && open $(html_report)
	@rm -f $(test_log) &> /dev/null

.PHONY: test-ci-unit
test-ci-unit: test-internal
	@which goveralls > /dev/null || go get -u -f github.com/mattn/goveralls
	goveralls -coverprofile=$(coverfile) -service=travis-ci || echo -e "\x1b[31mCoveralls failed\x1b[m"

.PHONY: test-ci-integration
test-ci-integration:
	$(test_ci_integration)

.PHONY: install-licence-bin
install-license-bin: install-vendor
	@echo Installing node modules
	[ -d $(license_node_modules) ] || (cd $(license_dir) && npm install)

.PHONY: generics-gen
generics-gen: install-generics-bin install-license-bin
	@echo Generating code from generic templates
	PACKAGE=$(package_root) $(auto_gen) $(generics_output_dir) $(generics_rules_dir)

.PHONY: clean
clean:
	@rm -f *.html *.xml *.out *.test

.PHONY: all
all: metalint test-ci-unit test-ci-integration eventdb
	@echo Made all successfully

.DEFAULT_GOAL := all
