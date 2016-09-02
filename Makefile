.PHONY: eva eva-base upload-eva upload-eva-base doc

test:
	nosetests

lint:
	flake8 eva/ --ignore=E501

check-git-clean:
	if test "`git status --porcelain | wc -l`" != "0"; then
		$(error Please clean up your working directory and push your changes before building EVA)
	fi

pull-eva-base:
	docker pull metno/eva-base

eva: check-git-clean test lint pull-eva-base
	docker build --no-cache --tag metno/eva docker/eva

eva-base:
	docker build --no-cache --tag metno/eva-base docker/eva-base

upload-eva:
	docker push metno/eva

upload-eva-base:
	docker push metno/eva-base

doc:
	doxygen doxygen.conf

all: eva-base eva
