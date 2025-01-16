update netflix-analytics-448017.ANALYTICS_NETFLIX.CFG_FLOW_MANAGER
set last_value = current_timestamp()
where FLOW_NAME = '{{ params.FLOW_NAME }}'