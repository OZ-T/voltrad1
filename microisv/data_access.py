
def read_docs_from_db(globalconf,log,collection_name):
    """
    """
    log.info("Reading collection " + collection_name  + " from mongo ... ")
    import pymongo
    client = pymongo.MongoClient()
    db_name = globalconf.config['mongo']['microisv_db']
    db = client[db_name]
    cursor1 = db[collection_name].find(filter={})
    list1 = []
    for doc in cursor1:
        #del doc['_id']
        doc['_id'] = str(doc['_id'])
        list1.append(doc)
    return list1 #['div'].values[0], df1['script'].values[0]

def delete_doc_to_db(globalconf, log,doc,collection_name):
    log.info("Delete data for collection " + collection_name  + "  in mongo ... ")
    import datetime as dt
    import pandas as pd
    import pymongo
    client = pymongo.MongoClient()
    db_name = globalconf.config['mongo']['microisv_db']
    db = client[db_name]
    ret1 = db[collection_name].delete_one(filter={'id':doc['id']})
    return1 = ret1
    return str(return1)


def update_timers_to_db(globalconf, log,timer, action,collection_name):
    log.info("Updating data for collection  " + collection_name  + " in mongo ... ")
    import datetime as dt
    import pandas as pd
    import pymongo
    client = pymongo.MongoClient()
    db_name = globalconf.config['mongo']['microisv_db']
    db = client[db_name]
    collection = db[collection_name]
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


def save_docs_to_db(globalconf, log,docs,collection_name):
    log.info("Appending data for collection  " + collection_name  + " to mongo ... ")
    import datetime as dt
    import pandas as pd
    import pymongo
    client = pymongo.MongoClient()
    db_name = globalconf.config['mongo']['microisv_db']
    db = client[db_name]
    collection = db[collection_name]
    ret1 = collection.insert_many([docs])
    return1 = ret1.inserted_ids
    # put the json inside a list to allow the insert and avoid the following error:
    #   TypeError: document must be an instance of dict, bson.son.SON, bson.raw_bson.RawBSONDocument,
    #   or a type that inherits from collections.MutableMapping
    return str(return1)
