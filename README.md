# EVA

This daemon listens for messages coming from Productstatus, creates jobs based on the messages, and then submits them to a processing engine such as a local thread or the Sun OpenGridEngine.

## Set up your build environment

```
virtualenv deps
source deps/bin/activate
pip install -e .
```

## Building a Docker container

```
# To compile:
make eva

# To upload to Docker registry:
make upload-eva
```

## Running tests

```
source deps/bin/activate
nosetests
```

## Building the documentation

```
make doc
```
