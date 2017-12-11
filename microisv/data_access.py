
def read_timers_from_db(globalconf,log):
    """
    """
    log.info("Reading timers from mongo ... ")
    import pymongo
    client = pymongo.MongoClient()
    db_name = globalconf.config['mongo']['microisv_db']
    db = client[db_name]
    collection = db.collection
    cursor1 = db.collection.find(filter={})
    list1 = []
    for doc in cursor1:
        #del doc['_id']
        doc['_id'] = str(doc['_id'])
        list1.append(doc)
    return list1 #['div'].values[0], df1['script'].values[0]


def delete_timers_to_db(globalconf, log, timer):
    log.info("Delete data for timers in mongo ... ")
    import datetime as dt
    import pandas as pd
    import pymongo
    client = pymongo.MongoClient()
    db_name = globalconf.config['mongo']['microisv_db']
    db = client[db_name]
    collection = db.collection
    ret1 = collection.delete_one(filter={'id':timer['id']})
    return1 = ret1
    return str(return1)


def update_timers_to_db(globalconf, log,timer, action):
    log.info("Updating data for timers in mongo ... ")
    import datetime as dt
    import pandas as pd
    import pymongo
    client = pymongo.MongoClient()
    db_name = globalconf.config['mongo']['microisv_db']
    db = client[db_name]
    collection = db.collection
    if action == 'start':
        timer['runningSince'] = timer['start']
        #del timer['start']
    elif action == 'stop':
        timerOld = collection.find_one(filter={'id': timer['id']})
        delta = timer['stop'] - timerOld['runningSince']
        #del timer['stop']
        timerOld['elapsed'] += delta
        timerOld['runningSince'] = None
        timer = timerOld

    ret1 = collection.find_one_and_update(filter={'id':timer['id']},update={"$set": timer}, upsert=True)
    return1 = ret1
    return str(return1)


def save_timers_to_db(globalconf, log,timer):
    log.info("Appending data for timers to mongo ... ")
    import datetime as dt
    import pandas as pd
    import pymongo
    client = pymongo.MongoClient()
    db_name = globalconf.config['mongo']['microisv_db']
    db = client[db_name]
    collection = db.collection
    ret1 = collection.insert_many([timer])
    return1 = ret1.inserted_ids
    # put the json inside a list to allow the insert and avoid the following error:
    #   TypeError: document must be an instance of dict, bson.son.SON, bson.raw_bson.RawBSONDocument,
    #   or a type that inherits from collections.MutableMapping
    return str(return1)
