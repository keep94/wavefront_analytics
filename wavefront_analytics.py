import datetime
import time
import wavefront_api_client

from wavefront_api_client.rest import ApiException


def run_queries(properties, wql_by_metric, log):

    # Needed so that returned generator remains unaffected if caller
    # later changes the wql_by_metric dictionary.
    return _run_queries(properties, dict(wql_by_metric), log)


def columns():
    return 'customer', 'cluster', 'ts', 'metric', 'value'


def _run_queries(properties, wql_by_metric, log):

    # truncate time to start of current hour
    end_time = (int(time.time()) // 3600) * 3600

    if 'start-time' in properties:
        start_time = _parse_time(properties['start-time'])
        if start_time < end_time:
            config = _build_configuration(properties)
            for metric, wql in wql_by_metric.items():
                rows = _wf_query(
                    config,
                    metric,
                    wql,
                    log,
                    start_time,
                    end_time)
                if rows:
                    yield rows
    properties['start-time'] = _format_time(end_time)


def _format_time(ts):

    # Make sure returned calendar time is in GMT.
    timeval=datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
    return timeval.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_time(s):
    dt = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")

    # dt isn't timeszone aware even though s ends in a Z. We have to assign
    # it the GMT timezone so that the seconds since epoch calculation works
    # correctly. Otherwise python does the seconds since epoch calculation as
    # if dt is in the local time zone.
    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())


def _build_configuration(properties):
    configuration = wavefront_api_client.Configuration()
    configuration.host = properties['wavefront-server']
    configuration.api_key['X-AUTH-TOKEN'] = properties['wavefront-token']
    return configuration


def _wf_query(configuration, metric, wql, log, start_time, end_time):
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
            log.warning(f'{metric}: Metrics not found. {api_response.error_message or api_response.warnings}')
            return []
        for series in api_response.timeseries:
            if getattr(series, 'tags', None) is None:
                log.warning(f'{metric}: customer and cluster tags missing')
                return []
            customer = series.tags.get('customer')
            cluster = series.tags.get('cluster')
            if customer is None or cluster is None:
                log.warning(f'{metric}: customer and cluster tags missing')
                return []
            for data in series.data:
                timestamp = int(data[0])
                value = data[1]
                result.append((customer, cluster, _format_time(timestamp), metric, value))
    except ApiException as e:
        log.warning(f'{metric}: Exception when calling QueryApi->query_api: {e.status} {e.reason}')
        return []
    return result
