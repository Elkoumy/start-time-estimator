
import time
from estimate_start_times.config import Configuration, ReEstimationMethod, ConcurrencyOracleType, \
    ResourceAvailabilityType, HeuristicsThresholds, OutlierStatistic, DEFAULT_CSV_IDS, EventLogIDs

from estimate_start_times.estimator import StartTimeEstimator
from estimate_start_times.utils import read_csv_log
import pandas as pd




def run_estimation(event_log_path, configuration, output_log_path):
    print("\nProcessing event log {}".format(event_log_path))
    # Read event log
    event_log = read_csv_log(event_log_path, configuration)
    # Process event log
    print("Starting start time estimation.")
    start_time = time.process_time()
    # Create start time estimator
    start_time_estimator = StartTimeEstimator(event_log, configuration)
    # Estimate start times
    extended_event_log = start_time_estimator.estimate()
    end_time = time.process_time()
    print("Estimation finished ({}s).".format(end_time - start_time))

    if configuration.log_ids.start_time in extended_event_log.columns:
        #Convert timestamp value to datetime
        extended_event_log[configuration.log_ids.start_time] = timestamp_to_string(
            extended_event_log[configuration.log_ids.start_time]
        )

    extended_event_log[configuration.log_ids.end_time] = timestamp_to_string(
        extended_event_log[configuration.log_ids.end_time]
    )
    extended_event_log[configuration.log_ids.enabled_time] = timestamp_to_string(
        extended_event_log[configuration.log_ids.enabled_time]
    )
    extended_event_log[configuration.log_ids.available_time] = timestamp_to_string(
        extended_event_log[configuration.log_ids.available_time]
    )
    extended_event_log[configuration.log_ids.estimated_start_time] = timestamp_to_string(
        extended_event_log[configuration.log_ids.estimated_start_time]
    )
    # Export
    extended_event_log.to_csv(output_log_path, encoding='utf-8', index=False)

def timestamp_to_string(dates: pd.Series) -> pd.Series:
    return (dates.apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f') if not pd.isnull(x) else '').apply(lambda x: x[:-3]) +
                dates.apply(lambda x: x.strftime("%z") if not pd.isnull(x) else '').apply(lambda x: x[:-2]) +
                ":" +
                dates.apply(lambda x: x.strftime("%z") if not pd.isnull(x) else '').apply(lambda x: x[-2:]))

def main():
    outlier_threshold = 1.0
    folder = "heur-median"



    config = Configuration(
        log_ids=EventLogIDs(case='case_id',
                              activity='Activity',
                              start_time='start_time',
                              end_time='end_time',
                              enabled_time='time:enabled',
                              available_time='time:available',
                              estimated_start_time='time:estimated_start',
                              resource='Resource'),
        re_estimation_method=ReEstimationMethod.MEDIAN,
        concurrency_oracle_type=ConcurrencyOracleType.HEURISTICS,
        resource_availability_type=ResourceAvailabilityType.SIMPLE,
        heuristics_thresholds=HeuristicsThresholds(df=0.1, l2l=0.1),
        instant_activities={"Send invoice", " Choose best option"},
        outlier_statistic=OutlierStatistic.MODE,
        outlier_threshold=outlier_threshold
    )
    run_estimation("../event_logs/Procure-to-Pay.csv", config,
                   "../event_logs/Procure-to-Pay_estimated_low_h.csv".format(folder))



if __name__ == '__main__':
    main()