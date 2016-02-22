.PHONY: eva eva-base upload-eva upload-eva-base

eva:
	docker build --no-cache --tag metno/eva:14.04 docker/eva

eva-base:
	docker build --no-cache --tag metno/eva-base:14.04 docker/eva-base

upload-eva:
	docker push metno/eva:14.04

upload-eva-base:
	docker push metno/eva-base:14.04

all: eva-base eva
