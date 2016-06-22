.PHONY: eva eva-base upload-eva upload-eva-base doc

pull-eva-base:
	docker pull metno/eva-base

eva: pull-eva-base
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
