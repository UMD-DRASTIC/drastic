import unittest

from indigo.models.activity import Activity

class ActivityTest(unittest.TestCase):

    def test_actvity_ordering(self):
        # Make sure the order we create them (oldest first) is
        # also the way we get the activities back from the DB.
        new_activities = [Activity.new("Random activity") for x in xrange(20)]
        activities = Activity.recent(10)

        # We want the most recent 10 from the new items we created ...
        new_activity_ids = [a.when for a in new_activities[10:]]
        new_activity_ids.reverse()

        activity_ids = [a.when for a in activities]


        assert activity_ids == new_activity_ids
