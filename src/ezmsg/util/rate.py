"""
Rate limiting utilities for ezmsg.

This module provides the Rate class for maintaining consistent timing in loops,
inspired by the ROS Rate feature. Supports both synchronous and asynchronous
sleep operations for rate-limited execution.

This is a modified version of the original script below:
https://github.com/ros/ros_comm/blob/noetic-devel/clients/rospy/src/rospy/timer.py
"""

import time
import asyncio


class Rate(object):
    """
    Convenience class for sleeping in a loop at a specified rate
    """

    def __init__(self, hz: float):
        """
        Constructor.
        @param hz: hz rate to determine sleeping
        @type  hz: float
        """
        self.last_time = time.time()
        self.sleep_dur = 1.0 / hz

    def _remaining(self, curr_time: float) -> float:
        """
        Calculate the time remaining for rate to sleep.

        :param curr_time: Current time
        :type curr_time: float
        :return: Time remaining in seconds
        :rtype: float
        """
        # detect time jumping backwards
        if self.last_time > curr_time:
            self.last_time = curr_time

        # calculate remaining time
        elapsed = curr_time - self.last_time
        return self.sleep_dur - elapsed

    def remaining(self) -> float:
        """
        Return the time remaining for rate to sleep.

        :return: Time remaining in seconds
        :rtype: float
        """
        curr_time = time.time()
        return self._remaining(curr_time)

    def _sleep_logic(self) -> float:
        """
        Internal method to calculate sleep time and update timing state.

        :return: Time to sleep in seconds (non-negative)
        :rtype: float
        """
        curr_time = time.time()
        time_remaining = self._remaining(curr_time)

        self.last_time = self.last_time + self.sleep_dur

        if curr_time - self.last_time > self.sleep_dur * 2:
            self.last_time = curr_time

        return time_remaining if time_remaining > 0 else 0

    async def sleep(self):
        """
        Asynchronously sleep for the appropriate duration to maintain the target rate.
        """
        await asyncio.sleep(self._sleep_logic())

    def sleep_sync(self):
        """
        Synchronously sleep for the appropriate duration to maintain the target rate.
        """
        time.sleep(self._sleep_logic())
