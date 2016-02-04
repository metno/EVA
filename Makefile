.PHONY: docker

docker:
	docker build --no-cache --tag metno/eva .
