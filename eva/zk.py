"""!
@brief ZooKeeper functions.
"""

import json

import eva.exceptions


# Try to keep message cache in ZooKeeper to a minimum; we use 1/4 of the max
# message size in ZooKeeper, which amounts to 250kB.
ZOOKEEPER_MSG_LIMIT = (1024 ** 2) / 4


def load_serialized_data(zookeeper, path, default=[]):
    """!
    @brief Load JSON serialized data from ZooKeeper.
    @returns The loaded data.
    """
    if zookeeper.exists(path):
        serialized = zookeeper.get(path)
        return json.loads(serialized[0].decode('ascii'))
    return default


def store_serialized_data(zookeeper, path, data):
    """!
    @brief Store structured data in ZooKeeper as a serialized JSON object.
    @returns A tuple of integers (num_items, total_byte_size)
    @throws kazoo.exceptions.ZooKeeperError on failure, or eva.exceptions.ZooKeeperDataTooLargeException if the message size is too large
    """
    serialized = json.dumps(data).encode('ascii')
    serialized_byte_size = len(serialized)
    if serialized_byte_size > ZOOKEEPER_MSG_LIMIT:
        raise eva.exceptions.ZooKeeperDataTooLargeException('Cannot store data in ZooKeeper since it exceeds the message limit of %d bytes.', ZOOKEEPER_MSG_LIMIT)
    if not zookeeper.exists(path):
        zookeeper.create(path, serialized)
    else:
        zookeeper.set(path, serialized)
    return (len(data), serialized_byte_size,)
