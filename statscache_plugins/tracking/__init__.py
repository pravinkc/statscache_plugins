import os

from statscache.plugins import BasePlugin, BaseModel
from datetime import datetime
import fedmsg.meta
import sqlalchemy as sa


class Model(BaseModel):
    __tablename__ = "data_popularity"
    references = sa.Column(sa.Integer, nullable=False)
    username = sa.Column(sa.UnicodeText, nullable=False, index=True)


class Plugin(BasePlugin):
    name = "tracking_users"
    summary = "Track the direct references to a list of usernames"
    description = """
    Define a enviornment varialbe TRACK_USERS to track the users
    """

    model = Model

    def __init__(self, *args, **kwargs):
        super(Plugin, self).__init__(*args, **kwargs)
        self.pending = []
        self.tracking = os.environ.get("TRACK_USERS").split(" ")

    def process(self, message):
        timestamp = datetime.fromtimestamp(message['timestamp'])
        for username in fedmsg.meta.msg2usernames(message):
            if username not in self.tracking:
                return
            self.pending.append((timestamp, username))

    def update(self, session):
        for (timestamp, username) in self.pending:
            previous = session.query(self.model)\
                       .filter(self.model.username == username)\
                       .order_by(self.model.timestamp.desc())\
                       .first()
            session.add(self.model(
                timestamp=timestamp,
                username=username,
                references=getattr(previous, 'references', 0) + 1
            ))
        self.pending = []
        session.commit()
