import collections
import datetime

from statscache_plugins.volume.utils import VolumePluginMixin, plugin_factory

import sqlalchemy as sa


class PluginMixin(VolumePluginMixin):
    name = "volume, by category"
    summary = "the count of messages, organized by category"
    description = """
    For any given time window, the number of messages that come across
    the bus for each category.
    """

    def handle(self, session, messages):
        volumes = collections.defaultdict(int)
        for msg in messages:
            msg_timestamp = datetime.datetime.fromtimestamp(msg['timestamp'])
            volumes[(msg['topic'].split('.')[3],
                     self.frequency.next(now=msg_timestamp))] += 1

        for key, volume in volumes.items():
            category, timestamp = key
            result = session.query(self.model)\
                .filter(self.model.category == category)\
                .filter(self.model.timestamp == timestamp)
            row = result.first()
            if row:
                row.volume += volume
            else:
                row = self.model(
                    timestamp=timestamp,
                    volume=volume,
                    category=category)
            session.add(row)
            session.commit()


plugins = plugin_factory(
    [datetime.timedelta(seconds=s) for s in [1, 5, 60]],
    PluginMixin,
    "VolumeByCategory",
    "data_volume_by_category_",
    columns={
        'volume': sa.Column(sa.Integer, nullable=False),
        'category': sa.Column(sa.UnicodeText, nullable=False, index=True),
    }
)
