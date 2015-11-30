CWD := $(shell pwd)
BUILDS    := .build
VERSION   := 1.0
DEPLOY    := $(BUILDS)/deploy
TARDIR    := skyring-ceph-provider-$(VERSION)
BIGFINDIR := bigfin
RPMBUILD  := $(HOME)/rpmbuild/
PRINT_STATUS = export EC=$$?; cd $(CWD); if [ "$$EC" -eq "0" ]; then printf "SUCCESS!\n"; else exit $$EC; fi

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
	@go get -t ./...

verifiers: vet lint fmt getdeps

vet:
	@echo "Doing $@"
	@go tool vet .

fmt:
	@echo "Doing $@"
	@bash $(PWD)/build-aux/gofmt.sh

lint:
	@echo "Doing $@"
	@golint .

test:
	@echo "Doing $@"
	@go test -v ./...

pybuild:
	@echo "Doing $@"
	@cd backend/salt/python; python setup.py build

build: verifiers pybuild test
	@echo "Doing $@"
	@go build -o ceph_provider

pyinstall:
	@echo "Doing $@"
	@cd backend/salt/python; python setup.py --quiet install --user
	@echo "INFO: You should set PYTHONPATH make it into effect"

saltinstall:
	@echo "Doing $@"
	@if ! cp -f salt/* /srv/salt/ 2>/dev/null; then \
		echo "ERROR: unable to install salt files. Install them manually by"; \
		echo "sudo cp -f salt/* /srv/salt/"; \
	fi

install: build pyinstall saltinstall
	@echo "Doing $@"
	@go install

rpm:
	@echo "Building RPM"
	mkdir -p $(DEPLOY)/latest $(HOME)/$(BUILDS)
	cd .. ;\
	cp -fr $(BIGFINDIR) $(TARDIR) ; \
	cp -f $(TARDIR)/backend/salt/python/setup.py $(TARDIR); \
	cp -fr $(TARDIR)/backend/salt/python/skyring $(TARDIR)/; \
	tar -zcf skyring-ceph-provider-$(VERSION).tar.gz $(TARDIR); \
	cp skyring-ceph-provider-$(VERSION).tar.gz $(RPMBUILD)/SOURCES
	rpmbuild -ba skyring-ceph-provider.spec
	$(PRINT_STATUS); \
	if [ "$$EC" -eq "0" ]; then \
		cp -f $(RPMBUILD)/RPMS/noarch/skyring-ceph-provider-$(VERSION)*.rpm $(DEPLOY)/latest/; \
		printf "\nThe Skyring Ceph Provider RPMs are located at:\n\n"; \
		printf "   $(DEPLOY)/latest\n\n\n\n"; \
	fi
