import datetime
import time
import wavefront_api_client

from wavefront_api_client.rest import ApiException


def run_query(properties, wql, log):
    end_time = (int(time.time()) // 3600) * 3600
    result = []
    if 'start-time' in properties:
        start_time = _parse_time(properties['start-time'])
        if start_time < end_time:
            result = _wf_query(
                _build_configuration(properties),
                wql,
                log,
                start_time,
                end_time)
    properties['start-time'] = _format_time(end_time)
    return result


def columns(timeseries_value_column_name):
    return 'customer', 'cluster', 'ts', timeseries_value_column_name


def _format_time(ts):
    timeval=datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
    return timeval.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_time(s):
    dt = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())


def _build_configuration(properties):
    configuration = wavefront_api_client.Configuration()
    configuration.host = properties['wavefront-server']
    configuration.api_key['X-AUTH-TOKEN'] = properties['wavefront-token']
    return configuration


def _wf_query(configuration, wql, log, start_time, end_time):
    """
    A function to query Wavefront for a piece of analytics data by customer,
    cluster and hourly timestamps. Returns a list of tuples to be uploaded to
    supercollider. Each tuple is of form (customer, cluster, timestamp_str, value).
    The returned data is for start_time inclusive and end_time exclusive.
    start_time and end_time are expected to be the top of an hour.
    """
    # create an instance of the API class
    api_instance = wavefront_api_client.QueryApi(wavefront_api_client.ApiClient(configuration))
    q = wql
    s = str(start_time)
    e = str(end_time)
    g = 'h' # str | the granularity of the points returned
    i = False # bool | whether series with only points that are outside of the query window will be returned (defaults to true) (optional)
    auto_events = False # bool | whether events for sources included in the query will be automatically returned by the query (optional)
    strict = True # bool | do not return points outside the query window [s;e), defaults to false (optional)
    include_obsolete_metrics = False # bool | include metrics that have not been reporting recently, defaults to false (optional)
    sorted = True # bool | sorts the output so that returned series are in order, defaults to false (optional) (default to false)
    cached = True # bool | whether the query cache is used, defaults to true (optional) (default to true)

    result = []
    try:
        # Perform a charting query against Wavefront servers that returns the appropriate points in the specified time window and granularity
        api_response = api_instance.query_api(q, s, g, e=e, i=i, auto_events=auto_events, strict=strict, include_obsolete_metrics=include_obsolete_metrics, sorted=sorted, cached=cached)
        if getattr(api_response, "timeseries", None) is None:
            raise Exception(f'Metrics not found. {api_response.warnings}')
        for series in api_response.timeseries:
            if getattr(series, 'tags', None) is None:
                raise Exception('customer and cluster tags missing')
            customer = series.tags.get('customer')
            cluster = series.tags.get('cluster')
            if customer is None or cluster is None:
                raise Exception('customer and cluster tags missing')
            for data in series.data:
                timestamp = int(data[0])
                value = data[1]
                result.append((customer, cluster, _format_time(timestamp), value))
    except ApiException as e:
        log.warning(f'Exception when calling QueryApi->query_api: {e.status} {e.reason}')
        raise
    return result
