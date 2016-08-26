# EVA

This daemon listens for messages coming from Productstatus, creates jobs based on the messages, and then submits them to a processing engine such as a local thread or the Sun OpenGridEngine.

## Set up your build environment

```
sudo apt-get install python-pip python-virtualenv python-dev
virtualenv deps
source deps/bin/activate
pip install -e .
```

## Configuration

Configuration of EVA is done via environment variables. See below for a detailed description of configuration variables.

### EVA_QUEUE_ORDER

Specify in which order to process incoming events.

#### FIFO
```export EVA_QUEUE_ORDER=FIFO```
FIFO is a first-in, first-out queue. The messages will be processed in chronological order. If messages appear out of order, they are sorted before processing. Note that if messages are severely delayed, they will be processed out of order anyway, because EVA does not know whether or not there are older messages on the queue. This is the default behavior.

#### LIFO
```export EVA_QUEUE_ORDER=LIFO```
LIFO is a last-in, first-out queue. It functions exactly as FIFO, but with reverse chronological order.

#### ADAPTIVE
```export EVA_QUEUE_ORDER=ADAPTIVE```
Messages will be processed in chronological order as with FIFO, but messages belonging to a ProductInstance will be checked for their reference time, and those with the most recent reference time will be processed first. This results in faster delivery of newer models in case of a service outage.

### EVA_INPUT_WITH_HASH

This variable controls whether an Adapter will process DataInstance resources containing a hash.
                
#### (null)
All resources will be processed. This is the default behavior.

#### YES
Only resources with a hash will be processed.

#### NO
Only resources lacking a hash will be processed.

## Writing adapters

See the file [eva/adapter/example.py] for an example.

## Running EVA

```
python -m eva --help
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
