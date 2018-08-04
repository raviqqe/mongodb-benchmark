"""MongoDB benchmark"""

import pymongo
import sys
import timeit
import uuid


def graph_lookup(collection, start_id):
    for user in collection.aggregate([
            {'$match': {'_id': start_id}},
            {'$graphLookup': {
                'from': 'users',
                        'startWith': '$friends',
                        'connectFromField': 'friends',
                        'connectToField': '_id',
                        'as': 'relevantPeople',
                        'maxDepth': 2,
            }}]):
        # print(user)
        pass


def main(num_users):
    client = pymongo.MongoClient()
    database = client.performance_test
    collection = database.users
    collection.delete_many({})

    ids = [uuid.uuid4().hex for _ in range(num_users)]

    collection.insert_many([{
        '_id': id,
        'friends': [friend_id for friend_id in ids if friend_id != id]
    } for id in ids])

    start_time = timeit.default_timer()

    graph_lookup(collection, ids[0])

    print('Query latency:', timeit.default_timer() - start_time)


if __name__ == "__main__":
    main(int(sys.argv[1]))
