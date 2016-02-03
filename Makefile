# store the current working directory
CWD := $(shell pwd)
PRINT_STATUS = export EC=$$?; cd $(CWD); if [ "$$EC" -eq "0" ]; then printf "SUCCESS!\n"; else exit $$EC; fi

VERSION   := 0.0.1
RELEASE   := 1
TARDIR    := ../bigfin-$(VERSION)
RPMBUILD  := $(HOME)/rpmbuild
BIGFIN_BUILD  := $(HOME)/.bigfin_build
BIGFIN_BUILD_SRC  := $(BIGFIN_BUILD)/golang/gopath/src/github.com/skyrings/bigfin
BIGFIN_BUILD_TARDIR := $(BIGFIN_BUILD)/golang/gopath/src/github.com/skyrings/bigfin/$(TARDIR)

all: install

checkdeps:
	@echo "Doing $@"
	@bash $(PWD)/build-aux/checkdeps.sh

getversion:
	@echo "Doing $@"
	@bash $(PWD)/build-aux/pkg-version.sh $(PWD)/version.go

getdeps: checkdeps getversion
	@echo "Doing $@"
	@go get github.com/golang/lint/golint
	@go get github.com/Masterminds/glide

verifiers: getdeps vet fmt lint

vet:
	@echo "Doing $@"
	@bash $(PWD)/build-aux/run-vet.sh

fmt:
	@echo "Doing $@"
	@bash $(PWD)/build-aux/gofmt.sh

lint:
	@echo "Doing $@"
	@golint .

test:
	@echo "Doing $@"
	@GO15VENDOREXPERIMENT=1 go test $$(GO15VENDOREXPERIMENT=1 glide nv)

pybuild:
	@echo "Doing $@"
	if [ "$$USER" == "root" ]; then \
                cd backend/salt/python; python setup.py --quite install --root /; cd -; \
        else \
                cd backend/salt/python; python setup.py --quiet install --user; cd -; \
        fi

vendor-update:
	@echo "Updating vendored packages"
	@GO15VENDOREXPERIMENT=1 glide -q up 2> /dev/null

build: getdeps verifiers pybuild test
	@echo "Doing $@"
	@GO15VENDOREXPERIMENT=1 go build -o ceph_provider

build-special:
	rm -fr $(BIGFIN_BUILD_SRC) $(BIGFIN_BUILD)
	mkdir $(BIGFIN_BUILD_SRC) -p
	cp -ai $(CWD)/* $(BIGFIN_BUILD_SRC)/
	cd $(BIGFIN_BUILD_SRC); \
	export GOROOT=/usr/lib/golang/; \
	export GOPATH=$(BIGFIN_BUILD)/golang/gopath; \
	cp -r $(BIGFIN_BUILD_SRC)/vendor/* $(BIGFIN_BUILD)/golang/gopath/src/ ; \
	export PATH=$(PATH):$(GOPATH)/bin:$(GOROOT)/bin; \
	go build
	cp $(BIGFIN_BUILD_SRC)/bigfin $(CWD)

pyinstall:
	@echo "Doing $@"
	@cd backend/salt/python; python setup.py --quiet install --user
	@echo "INFO: You should set PYTHONPATH make it into effect"

saltinstall:
	@echo "Doing $@"
	@if ! cp -f salt/* /srv/salt/ 2>/dev/null; then \
		echo "ERROR: unable to install salt files. Install them manually by"; \
		echo "    sudo cp -f backend/salt/sls/* /srv/salt/"; \
	fi

install: build pyinstall saltinstall
	@echo "Doing $@"
	@GO15VENDOREXPERIMENT=1 go install

dist:
	@echo "Doing $@"
	rm -fr $(TARDIR)
	mkdir -p $(TARDIR)
	rsync -r --exclude .git/ $(CWD)/ $(TARDIR)
	tar -zcf $(TARDIR).tar.gz $(TARDIR);

rpm:    dist
	@echo "Doing $@"
	rm -rf $(RPMBUILD)/SOURCES
	mkdir -p $(RPMBUILD)/SOURCES
	cp ../bigfin-$(VERSION).tar.gz $(RPMBUILD)/SOURCES; \
	rpmbuild -ba bigfin.spec
	$(PRINT_STATUS); \
	if [ "$$EC" -eq "0" ]; then \
		FILE=$$(readlink -f $$(find $(RPMBUILD)/RPMS -name bigfin-$(VERSION)*.rpm)); \
		cp -f $$FILE $(BIGFIN_BUILD)/; \
		printf "\nThe Bigfin RPMs are located at:\n\n"; \
		printf "   $(BIGFIN_BUILD)/\n\n\n\n"; \
	fi
