"""Gif Database

Usage:
    database.py (add|update) <url> <tags>...
    database.py search <tags>...
    database.py list items
    database.py list tags
    database.py init
    database.py (-h | --help)

Options:
    -h --help     Show this screen.

"""
import sqlite3
import argparse
from docopt import docopt
from pprint import pprint

class Item(object):
    def __init__(self, row):
        self.itemid = row[0]
        self.description = row[1]
        self.url = row[2]

    def display(self):
        print 'id: {}, url: {}'.format(self.itemid, self.url)
        print 'desc: {}'.format(self.description)

class gifdb(object):

    def __init__(self, filename='gif.db'):
        self.filename = filename
        self.conn = None
        self.cursor = None

    def init_db(self):
        conn = sqlite3.connect(self.filename)
        c = conn.cursor()

        c.execute('''CREATE TABLE ITEMS(
            itemid INT PRIMARY KEY     NOT NULL,
            description TEXT,
            url CHAR(200)              NOT NULL
        );''')

        c.execute('''CREATE TABLE TAGS(
            tagid INT PRIMARY KEY     NOT NULL,
            label CHAR(200)           NOT NULL
        );''')

        c.execute('''CREATE TABLE ITEMTAGS(
            itemid INT,
            tagid INT,
            FOREIGN KEY(itemid) REFERENCES ITEMS(itemid),
            FOREIGN KEY(tagid) REFERENCES TAGS(tagid)
        );''')

        c.execute('INSERT INTO TAGS (tagid, label) VALUES (0, "gif")')
        c.execute('INSERT INTO ITEMS (itemid, description, url) VALUES (0, "Dummy Item", "")')
        conn.commit()

        print "Initialized database successfully"
        conn.close()

    def connect(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.filename)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()

    def close(self):
        if self.conn is not None:
            self.conn.close()
        self.conn = None
        self.cursor = None

    def __del__(self):
        self.close()

    def get_tag_id(self, label):
        rows = self.cursor.execute(
            '''SELECT tagid FROM TAGS WHERE label=?''',
            (label,)
        ).fetchall()
        if len(rows) == 0:
            return None
        if len(rows) > 1:
            print 'Warning: multiple ids found for tag label: {}'.format(label)
        return rows[0]['tagid']

    def tag_exists(self, label):
        if self.get_tag_id(label) is not None:
            return True
        return False

    def add_tag(self, label):
        if self.tag_exists(label):
            return None

        rows = self.cursor.execute("SELECT MAX(tagid) AS tagid FROM TAGS").fetchall()
        if len(rows) == 0:
            new_id = 0
        else:
            new_id = rows[0]['tagid'] + 1

        self.cursor.execute("INSERT INTO TAGS (tagid, label) VALUES (?, ?)", (new_id, label))
        self.conn.commit()

        return new_id

    def list_tags(self):
        rows = self.cursor.execute("SELECT label FROM TAGS").fetchall()
        return [ r['label'] for r in rows ]

    def add(self, url, tags, description=''):

        rows = self.cursor.execute("SELECT * FROM ITEMS WHERE url=?", (url,)).fetchall()
        if rows:
            print "url already exists"
            return

        rows = self.cursor.execute("SELECT MAX(itemid) AS itemid FROM ITEMS").fetchall()
        if len(rows) == 0:
            new_id = 0
        else:
            new_id = rows[0]['itemid'] + 1

        for t in tags:
            self.add_tag(t)

        for t in tags:
            tid = self.get_tag_id(t)
            self.cursor.execute("INSERT INTO ITEMTAGS (itemid, tagid) VALUES (?, ?)", (new_id, tid))
            self.conn.commit()

        self.cursor.execute("INSERT INTO ITEMS (itemid, description, url) VALUES (?, ?, ?)", (new_id, description, url))
        self.conn.commit()

    def update(self, url, tags, description=''):

        rows = self.cursor.execute("DELETE FROM ITEMS WHERE url=?", (url,)).fetchall()
        self.add(url, tags, description)

    def search(self, tags):
        tag_ids = [ self.get_tag_id(t) for t in tags ]
        rows = self.cursor.execute("SELECT itemid FROM ITEMTAGS").fetchall()
        ritemids = set([row['itemid'] for row in rows])
        for t in tags:
            rows = self.cursor.execute(
                "SELECT itemid FROM ITEMTAGS NATURAL JOIN TAGS WHERE label=?",
                (t,)
            ).fetchall()
            ritemids &= set([row['itemid'] for row in rows])

        rows = []
        for iid in ritemids:
            rows += self.cursor.execute("SELECT * FROM ITEMS WHERE itemid=?", (iid,)).fetchall()
        for row in rows:
            print row

    def list_items(self):
        rows = self.cursor.execute("SELECT * FROM ITEMS").fetchall()
        for row in rows:
            print row

    def list_tags(self):
        pprint([r[1] for r in self.cursor.execute("SELECT * FROM TAGS").fetchall()])

if __name__ == '__main__':
    arguments = docopt(__doc__)
    db = gifdb()
    if arguments['init']:
        db.init_db()
    else:
        db.connect()

    if arguments['search']:
        db.search(arguments['<tags>'])
    if arguments['add']:
        db.add(arguments['<url>'], arguments['<tags>'], " ".join(arguments['<tags>']))
    if arguments['update']:
        db.update(arguments['<url>'], arguments['<tags>'], " ".join(arguments['<tags>']))
    if arguments['list'] and arguments['items']:
        db.list_items()
    if arguments['list'] and arguments['tags']:
        db.list_tags()
