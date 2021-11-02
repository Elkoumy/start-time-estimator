from statistics import mode
from typing import Union

import numpy as np
import pandas as pd
from pm4py.objects.log.obj import EventLog

from common import EventLogType
from config import ConcurrencyOracleType, ReEstimationMethod, ResourceAvailabilityType
from data_frame.concurrency_oracle import AlphaConcurrencyOracle as DFAlphaConcurrencyOracle
from data_frame.concurrency_oracle import ConcurrencyOracle as DFConcurrencyOracle
from data_frame.concurrency_oracle import NoConcurrencyOracle as DFNoConcurrencyOracle
from data_frame.resource_availability import ResourceAvailability as DFResourceAvailability
from event_log.concurrency_oracle import AlphaConcurrencyOracle as ELAlphaConcurrencyOracle
from event_log.concurrency_oracle import ConcurrencyOracle as ELConcurrencyOracle
from event_log.concurrency_oracle import NoConcurrencyOracle as ELNoConcurrencyOracle
from event_log.resource_availability import ResourceAvailability as ELResourceAvailability


class StartTimeEstimator:
    def __init__(self, event_log, config):
        # Set event log
        self.event_log = event_log
        # Set configuration
        self.config = config
        # Set type of event log
        if type(event_log) is EventLog:
            self.event_log_type = EventLogType.EVENT_LOG
        elif type(event_log) is pd.DataFrame:
            self.event_log_type = EventLogType.DATA_FRAME
        else:
            print("Unrecognizable event log instance!! Only Pandas-DataFrame and PM4PY-EventLog are supported.")
            self.event_log_type = EventLogType.EVENT_LOG
        # Set concurrency oracle instance
        self.concurrency_oracle = self._set_concurrency_oracle()
        # Set resource availability instance
        self.resource_availability = self._set_resource_availability()

    def _set_concurrency_oracle(self) -> Union[DFConcurrencyOracle, ELConcurrencyOracle]:
        if self.config.concurrency_oracle_type == ConcurrencyOracleType.NONE:
            # If selected type is NONE: instantiate either DataFrame-NoConcurrencyOracle or EventLog-NoConcurrencyOracle
            concurrency_oracle = DFNoConcurrencyOracle(self.event_log, self.config) \
                if self.event_log_type == EventLogType.DATA_FRAME \
                else ELNoConcurrencyOracle(self.event_log, self.config)
        elif self.config.concurrency_oracle_type == ConcurrencyOracleType.ALPHA:
            # If selected type is ALPHA: instantiate either DataFrame-AlphaConcurrencyOracle or EventLog-AlphaConcurrencyOracle
            concurrency_oracle = DFAlphaConcurrencyOracle(self.event_log, self.config) \
                if self.event_log_type == EventLogType.DATA_FRAME \
                else ELAlphaConcurrencyOracle(self.event_log, self.config)
        else:
            # If none of the above, notify!
            print("No concurrency oracle defined! Setting Alpha as default.")
            # And instantiate either DataFrame-AlphaConcurrencyOracle or EventLog-AlphaConcurrencyOracle
            concurrency_oracle = DFAlphaConcurrencyOracle(self.event_log, self.config) \
                if self.event_log_type == EventLogType.DATA_FRAME \
                else ELAlphaConcurrencyOracle(self.event_log, self.config)
        return concurrency_oracle

    def _set_resource_availability(self) -> Union[DFResourceAvailability, ELResourceAvailability]:
        if self.config.resource_availability_type == ResourceAvailabilityType.SIMPLE:
            # If selected type is SIMPLE: instantiate either DataFrame-ResourceAvailability or EventLog-ResourceAvailability
            resource_availability = DFResourceAvailability(self.event_log, self.config) \
                if self.event_log_type == EventLogType.DATA_FRAME \
                else ELResourceAvailability(self.event_log, self.config)
        else:
            # If none of the above, notify!
            print("No resource availability defined! Setting Simple as default.")
            # And instantiate either DataFrame-ResourceAvailability or EventLog-ResourceAvailability
            resource_availability = DFResourceAvailability(self.event_log, self.config) \
                if self.event_log_type == EventLogType.DATA_FRAME \
                else ELResourceAvailability(self.event_log, self.config)
        return resource_availability

    def estimate(self) -> Union[EventLog, pd.DataFrame]:
        if self.event_log_type == EventLogType.DATA_FRAME:
            return self._estimate_data_frame()
        else:
            return self._estimate_event_log()

    def _estimate_data_frame(self) -> pd.DataFrame:
        # If there is not column for start timestamp, create it
        if self.config.log_ids.start_timestamp not in self.event_log.columns:
            self.event_log[self.config.log_ids.start_timestamp] = pd.NaT
        # Assign start timestamps
        for (key, trace) in self.event_log.groupby([self.config.log_ids.case]):
            for index, event in trace.iterrows():
                enabled_time = self.concurrency_oracle.enabled_since(trace, event)
                available_time = self.resource_availability.available_since(
                    event[self.config.log_ids.resource],
                    event[self.config.log_ids.end_timestamp]
                )
                self.event_log.loc[index, self.config.log_ids.start_timestamp] = max(enabled_time, available_time)
        # Fix start times for those events being the first one of the trace and the resource (with non_estimated_time)
        if self.config.re_estimation_method == ReEstimationMethod.SET_INSTANT:
            estimated_event_log = self._set_instant_non_estimated_start_times_data_frame()
        elif self.config.re_estimation_method == ReEstimationMethod.MODE:
            estimated_event_log = self._re_estimate_non_estimated_start_times_data_frame()
        else:
            print("Unselected re-estimation method for events with no estimated start time! Setting them as instant by default.")
            estimated_event_log = self._set_instant_non_estimated_start_times_data_frame()
        # Return modified event log
        return estimated_event_log

    def _set_instant_non_estimated_start_times_data_frame(self) -> pd.DataFrame:
        # Identify events with non_estimated as start time
        # and set their processing time to instant
        self.event_log[self.config.log_ids.start_timestamp] = np.where(
            self.event_log[self.config.log_ids.start_timestamp] == self.config.non_estimated_time,
            self.event_log[self.config.log_ids.end_timestamp],
            self.event_log[self.config.log_ids.start_timestamp]
        )
        # Return modified event log
        return self.event_log

    def _re_estimate_non_estimated_start_times_data_frame(self) -> pd.DataFrame:
        # Store the durations of the estimated ones
        activity_processing_times = self.event_log[self.event_log[self.config.log_ids.start_timestamp] != self.config.non_estimated_time] \
            .groupby([self.config.log_ids.activity]) \
            .apply(lambda row: row[self.config.log_ids.end_timestamp] - row[self.config.log_ids.start_timestamp])
        # Identify events with non_estimated as start time
        non_estimated_events = self.event_log[self.event_log[self.config.log_ids.start_timestamp] == self.config.non_estimated_time]
        for index, non_estimated_event in non_estimated_events.iterrows():
            activity = non_estimated_event[self.config.log_ids.activity]
            if activity in activity_processing_times:
                self.event_log.loc[index, self.config.log_ids.start_timestamp] = \
                    non_estimated_event[self.config.log_ids.end_timestamp] - mode(activity_processing_times[activity])
            else:
                # If this activity has no estimated times set as instant activity
                self.event_log.loc[index, self.config.log_ids.start_timestamp] = self.event_log.loc[
                    index, self.config.log_ids.end_timestamp]
        # Return modified event log
        return self.event_log

    def _estimate_event_log(self) -> EventLog:
        # Assign start timestamps
        for trace in self.event_log:
            for event in trace:
                enabled_time = self.concurrency_oracle.enabled_since(trace, event)
                available_time = self.resource_availability.available_since(
                    event[self.config.log_ids.resource],
                    event[self.config.log_ids.end_timestamp]
                )
                event[self.config.log_ids.start_timestamp] = max(
                    enabled_time,
                    available_time
                )
        # Fix start times for those events being the first one of the trace and the resource (with non_estimated_time)
        if self.config.re_estimation_method == ReEstimationMethod.SET_INSTANT:
            estimated_event_log = self._set_instant_non_estimated_start_times_event_log()
        elif self.config.re_estimation_method == ReEstimationMethod.MODE:
            estimated_event_log = self._re_estimate_non_estimated_start_times_event_log()
        else:
            print("Unselected fix method for events with no estimated start time! Setting them as instant by default.")
            estimated_event_log = self._set_instant_non_estimated_start_times_event_log()
        # Return modified event log
        return estimated_event_log

    def _set_instant_non_estimated_start_times_event_log(self) -> EventLog:
        # Identify events with non_estimated as start time
        # and set their processing time to instant
        for trace in self.event_log:
            for event in trace:
                if event[self.config.log_ids.start_timestamp] == self.config.non_estimated_time:
                    # Non-estimated, save event to estimate based on statistics
                    event[self.config.log_ids.start_timestamp] = event[self.config.log_ids.end_timestamp]
        # Return modified event log
        return self.event_log

    def _re_estimate_non_estimated_start_times_event_log(self) -> EventLog:
        # Identify events with non_estimated as start time
        # and store the durations of the estimated ones
        non_estimated_events = []
        activity_times = {}
        for trace in self.event_log:
            for event in trace:
                if event[self.config.log_ids.start_timestamp] == self.config.non_estimated_time:
                    # Non-estimated, save event to estimate based on statistics
                    non_estimated_events += [event]
                else:
                    # Estimated, store estimated time to calculate statistics
                    activity = event[self.config.log_ids.activity]
                    processing_time = event[self.config.log_ids.end_timestamp] - event[self.config.log_ids.start_timestamp]
                    if activity not in activity_times:
                        activity_times[activity] = [processing_time]
                    else:
                        activity_times[activity] += [processing_time]
        # Set as start time the end time - the mode of the processing times (most frequent processing time)
        for event in non_estimated_events:
            activity = event[self.config.log_ids.activity]
            if activity in activity_times:
                event[self.config.log_ids.start_timestamp] = event[self.config.log_ids.end_timestamp] - mode(activity_times[activity])
            else:
                # If this activity has no estimated times set as instant activity
                event[self.config.log_ids.start_timestamp] = event[self.config.log_ids.end_timestamp]
        # Return modified event log
        return self.event_log
