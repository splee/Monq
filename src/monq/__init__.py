from exc import CommandError
from datetime import datetime
from pymongo.bson import SON
import pymongo

class MonQueue(object):

    default_insert = dict(priority=0,
                          attempts=0,
                          locked_by=None,
                          locked_at=None,
                          last_error=None)

    sort_hash = SON(priority=-1)

    def __init__(self, connection, database="mongo_queue",
                 collection="mongo_queue", timeout=300, attempts=3):
        self.connection = connection

        self._database = database
        self._collection = collection
        self.timeout = timeout
        self.attempts = attempts

        # make sure we have indexes
        self.collection.ensure_index([('locked_by', pymongo.ASCENDING),
                                      ('locked_at', pymongo.ASCENDING)])
        self.collection.ensure_index([('locked_by', pymongo.ASCENDING),
                                      ('attempts', pymongo.ASCENDING)])

    @property
    def db(self):
        return self.connection[self._database]

    @property
    def collection(self):
        return self.db[self._collection]

    def flush(self):
        self.collection.drop()

    def insert(self, job):
        job_data = job.copy()
        job_data.update(self.default_insert)
        job_id = self.collection.insert(job_data)
        return self.collection.find_one({'_id': job_id})

    def lock_next(self, locked_by):
        cmd = SON()
        cmd['findandmodify'] = self._collection
        cmd['update'] = {'$set': {'locked_by': locked_by,
                                  'locked_at': datetime.utcnow()}
                        }
        cmd['query'] = {'locked_by': None,
                        'attempts': {'$lt': self.attempts}}
        cmd['sort'] = self.sort_hash
        cmd['limit'] = 1
        cmd['new'] = True
        return self.command(cmd)

    def cleanup(self):
        q = dict(locked_by='/.*/',
                 attempts={'$lt': self.attempts})
        res = self.collection.find(q)

        for job in res:
            self.release(job, job['locked_by'])

    def release(self, job, locked_by):
        cmd = SON()
        cmd['findandmodify'] = self_collection
        cmd['update'] = {'$set': {'locked_by': None,
                                  'locked_at': None}}
        cmd['query'] = {'locked_by': locked_by,
                        '_id': job['_id']}
        cmd['limit'] = 1
        cmd['new'] = True
        return self.command(cmd)

    def complete(self, job, locked_by):
        cmd = SON()
        cmd['findandmodify'] = self._collection
        cmd['query'] = {'locked_by': locked_by,
                        '_id': job['_id']}
        cmd['remove'] = True
        cmd['limit'] = 1
        return self.command(cmd)

    def error(self, job, error_message=None):
        job['attempts'] += 1
        job['last_error'] = error_message
        job['locked_by'] = None
        job['locked_at'] = None
        self.collection.save(job)

    def command(self, cmd):
        res = self.db.command(cmd)
        if not res['ok'] == 1.0:
            raise CommandError("Result was not OK: %s" % res)
        return res['value']
