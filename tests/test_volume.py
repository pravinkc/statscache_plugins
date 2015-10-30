import unittest
import datetime
import statscache.utils
import statscache.plugins
import statscache.plugins.schedule
import statscache_plugins.volume.by_category


class TestVolumePlugin(unittest.TestCase):

    def setUp(self):
        self.config = {
            # Consumer stuff
            "statscache.consumer.enabled": True,
            "statscache.sqlalchemy.uri": "sqlite:////var/tmp/statscache-dev-db.sqlite",
        }

    def _make_session(self):
        uri = self.config['statscache.sqlalchemy.uri']
        statscache.utils.create_tables(uri)
        return statscache.utils.init_model(uri)

    def test_init(self):
        schedule = statscache.plugins.schedule.Schedule(
            interval=datetime.timedelta(minutes=1))
        plugins = statscache_plugins.volume.by_category.plugins
        plugin = list(plugins)[0](schedule, self.config)
        #session = self._make_session()
        #plugin.fill(session) ... # we should test things like this and other behaviors.

if __name__ == '__main__':
    unittest.main()
