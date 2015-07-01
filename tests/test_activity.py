import unittest

from indigo.models.activity import Activity

def new_random_activity():
    return Activity.create(html="Random activity")

class ActivityTest(unittest.TestCase):

    def test_actvity_ordering(self):
        new_activities = [new_random_activity() for x in xrange(20)]
        activities = Activity.recent(10)

        new_activity_ids = [a.when for a in new_activities]
        activity_ids = [a.when for a in activities]

        assert activity_ids == new_activity_ids[:10]
