import unittest
import statscache.plugins
import statscache_plugins.volume.by_category
from sqlalchemy import create_engine


class TestVolumePlugin(unittest.TestCase):

    def setUp(self):
        self.config = {
            # Consumer stuff
            "statscache.consumer.enabled": True,
            "statscache.sqlalchemy.uri": "sqlite:////var/tmp/statscache-dev-db.sqlite",
        }

    def _make_session(self):
        uri = self.config['statscache.sqlalchemy.uri']
        statscache.plugins.create_tables(uri)
        return statscache.plugins.init_model(uri)

    def test_init(self):
        plugin = statscache_plugins.volume.by_category.OneMinuteFrequencyPlugin(self.config)
        session = self._make_session()
        plugin.initialize(session)

if __name__ == '__main__':
    unittest.main()