.PHONY: test lint check-git-clean pull-eva-base eva eva-base upload-eva upload-eva-base doc

test:
	nosetests

lint:
	flake8 eva/ --ignore=E501

check-git-clean:
	ifeq ($(shell test "`git status --porcelain | wc -l`" != "0"; echo $$?), 1)
		$(error Please clean up your working directory and push your changes before building EVA)
	endfi

pull-eva-base:
	docker pull metno/eva-base

eva: test lint pull-eva-base
	docker build --no-cache --tag metno/eva docker/eva

staging-eva: test lint pull-eva-base
	docker build --no-cache --tag metno/eva:staging docker/eva

eva-base:
	docker build --no-cache --tag metno/eva-base docker/eva-base

upload-eva:
	docker push metno/eva

upload-eva-base:
	docker push metno/eva-base

upload-staging-eva:
	docker push metno/eva-base:staging

doc:
	doxygen doxygen.conf

all: eva-base eva
