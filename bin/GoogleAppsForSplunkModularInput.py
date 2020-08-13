from __future__ import absolute_import
import sys
from splunk.appserver.mrsparkle.lib.util import make_splunkhome_path

_APP_NAME = 'GSuiteForSplunk'
import os.path

sys.path.insert(0, make_splunkhome_path(["etc", "apps", _APP_NAME, "bin", "lib"]))
sys.path.insert(0, make_splunkhome_path(["etc", "apps", _APP_NAME, "bin", "lib", "python3.7", "site-packages"]))

import contextlib
import datetime as dt
from datetime import timedelta, datetime
import time
import io

from apiclient.discovery import build
import google.oauth2.credentials
import google_auth_httplib2

import httplib2
import socks
from ModularInput import ModularInput
from Utilities import Utilities
from apiclient import errors
from json import dumps
import uuid

# SYSTEM EXIT CODES
_SYS_EXIT_FAILED_VALIDATION = 7
_SYS_EXIT_FAILED_GET_OAUTH_CREDENTIALS = 6
_SYS_EXIT_FAILURE_FIND_API = 5
_SYS_EXIT_OAUTH_FAILURE = 4


class GoogleAppsForSplunkModularInput(ModularInput):
    def __init__(self, app_name="NO NAME SPECIFIED", scheme={}):
        ModularInput.__init__(self, app_name, scheme)
        self.__http = None
        self.__proxy_info = None
        self.bq_client = None
        self._app_local_directory = None

    @property
    def available_apis(self):
        # REMOVED "usage": ["customer", "user"]
        return {"report": ["all", "groups", "mobile", "admin", "calendar", "drive", "login", "token", "rules", "saml",
                           "gplus", "chat", "groups_enterprise", "access_transparency", "user_accounts", "gcp",
                           "jamboard",
                           "chat", "meet"],
                "analytics": ["report", "metadata"],
                "admin": ["users"],
                "mail": ["metadata"],
                "alerts": ["all", "takeout", "gmail", "identity", "operations", "state", "mobile"]}

    @property
    def http_session(self):
        return self.__http

    @http_session.setter
    def http_session(self, http):
        self.__http = http

    def set_logger(self, log):
        self.log = log

    def _partition(self, lst, n):
        division = len(lst) / float(n)
        print("division :%s" % division)
        return [lst[int(round(division * i)): int(round(division * (i + 1)))] for i in range(n)]

    def _get_uuid(self):
        return uuid.uuid1()

    def _format_date(self, dS):
        if dS == "today":
            return dt.datetime.today().strftime("%Y-%m-%d")
        if dS == "yesterday":
            return (dt.date.today() - dt.timedelta(1)).strftime("%Y-%m-%d")
        return dS

    def _build_report_request(self, attr, metrics):
        a = dict(attr)
        a["metrics"] = metrics
        return a

    def google_analytics_api_metadata(self, **kwargs):
        self.info("function=google_analytics_api_metadata")
        analytics = build('analytics', 'v3', http=self.http_session)
        results = analytics.metadata().columns().list(reportType='ga').execute()
        columns = results.get('items', [])
        report_uuid = self._get_uuid()
        for column in columns:
            column_event = ""
            column_event += 'tracking_uuid="%s" column_id="%s" kind="%s"' % (
                report_uuid, column.get('id'), column.get('kind'))
            column_attributes = column.get('attributes', [])
            for name, value in column_attributes.iteritems():
                column_event += ' {0}="{1}"'.format(name, value)
            self.print_event(column_event)

    def _sanitize(self, s):
        return s.replace('\\u200e', "__u200e__").replace('\\u2010', "__u2010__")

    def google_analytics_api_reports(self, **kwargs):
        try:
            self.info("function=google_analytics_api_reports")
            checkpoint = kwargs["checkpoint"]
            self.log.debug("got_checkpoint={}".format(checkpoint))
            DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
            analytics = build('analytics', 'v4', http=self.http_session, discoveryServiceUrl=DISCOVERY_URI)
            if len(kwargs["dimensions"]) > 7:
                raise Exception("Dimensions can contain a max of 7 items")
            metrics = [kwargs["metrics"]]
            if len(kwargs["metrics"]) > 50:
                raise Exception("Please limit metrics to 50 total.")
            if len(metrics[0]) > 10:
                metrics = self._partition(metrics[0], len(metrics[0]) % 10)
            date_ranges = {}
            selected_dates = []
            if "start_date" in kwargs:
                date_ranges["startDate"] = self._format_date(kwargs["start_date"])
            if "end_date" in kwargs:
                date_ranges["endDate"] = self._format_date(kwargs["end_date"])
            running_backfill = False
            if checkpoint["is_backfill"]:
                self.log.debug("setting backfill to true")
                running_backfill = True
            time_ranges = []
            if not running_backfill:
                self.log.debug("not running backfill - creating time_ranges")
                time_ranges = ['{1}T{0:02d}:{2:d}'.format(x, date_ranges["startDate"], y) for x in range(0, 24) for y in
                               range(0, 6)]
                [time_ranges.append('NOT_T{0:02d}:{1:d}'.format(x, y, date_ranges["startDate"])) for x in range(0, 24)
                 for y in range(0, 6)]
                selected_dates = [date_ranges["startDate"]]
                self.log.debug("non-backfill selected_dates={}".format(selected_dates))
            elif running_backfill:
                self.log.debug("running backfill")
                start_backfill = kwargs["start_date"].split("-")
                end_backfill = kwargs["end_date"].split("-")
                completed_days = checkpoint["completed_days"]
                # dt.date.today() - dt.timedelta(1)).strftime("%Y-%m-%d")
                start_date = dt.date(int(start_backfill[0]), int(start_backfill[1]), int(start_backfill[2]))
                end_date = dt.date(int(end_backfill[0]), int(end_backfill[1]), int(end_backfill[2]))
                full_days = (end_date - start_date).days
                full_dates = [str(end_date - dt.timedelta(days=x)) for x in range(0, full_days)]
                self.log.debug("running_backfill start={} end={} days={} full_range={}".format(start_date, end_date,
                                                                                               full_days, full_dates))
                select_days = kwargs["backfill"]["days"]
                non_completed_days = [x for x in full_dates if x not in completed_days]
                selected_dates = non_completed_days[:select_days]
                self.log.debug("running_backfill selected_days={}".format(selected_dates))
                time_ranges = ['{1}T{0:02d}:{2:d}'.format(x, z, y) for x in range(0, 24) for y in
                               range(0, 6) for z in selected_dates]
                [time_ranges.append('NOT_T{0:02d}:{1:d}'.format(x, y)) for x in range(0, 24)
                 for y in range(0, 6) for z in selected_dates]
                # Seems, backwards, doesn't it? It is, just leave it.
                self.log.debug("running_backfill time_ranges={}".format(time_ranges))
                date_ranges["startDate"] = selected_dates[-1]
                date_ranges["endDate"] = selected_dates[0]
                # return
            run_uuid = self._get_uuid()
            import re
            for time_range in time_ranges:
                time.sleep(1)
                dimensionFilterClauses = []
                try:
                    # Here be Giants, Goblins, and Trolls, change these and lose your soul.
                    if "time_dimension" in kwargs:
                        if "NOT" in time_range:
                            dimensionFilterClauses = [
                                {"operator": "AND",
                                 "filters": [
                                     {"operator": "PARTIAL", "dimensionName": kwargs["time_dimension"],
                                      "expressions": [time_range.replace("NOT_", "")]}
                                 ]}]
                            for dr in selected_dates:
                                dimensionFilterClauses[0]["filters"].append({"operator": "BEGINS_WITH", "not": True,
                                                                             "dimensionName": kwargs["time_dimension"],
                                                                             "expressions": [dr]})
                        else:
                            dimensionFilterClauses = [
                                {"filters": [{"operator": "BEGINS_WITH", "dimensionName": kwargs["time_dimension"],
                                              "expressions": [time_range]}]}]
                except:
                    pass
                self.log.debug("dimensionFilter: %s" % dumps(dimensionFilterClauses))
                starting_report_request_attributes = {
                    'viewId': kwargs["view_id"],
                    'dateRanges': [date_ranges],
                    'dimensions': kwargs["dimensions"],
                    'samplingLevel': "LARGE",
                    'pageSize': 10000
                }
                if len(dimensionFilterClauses) > 0:
                    starting_report_request_attributes['dimensionFilterClauses'] = dimensionFilterClauses
                report_requests = []
                for x in metrics:
                    report_requests.append(self._build_report_request(starting_report_request_attributes, x))
                if len(report_requests) > 5:
                    raise Exception("Cannot have more than 5 report requests in a single call")
                report_response = analytics.reports().batchGet(
                    body={
                        'reportRequests': report_requests
                    }).execute()
                for report in report_response["reports"]:
                    columnHeader = report.get('columnHeader', {})
                    dimensionHeaders = columnHeader.get('dimensions', [])
                    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
                    rows = report.get('data', {}).get('rows', [])
                    is_data_golden = report.get('data', {}).get('isDataGolden', False)
                    self.log.debug("found {0} rows of data".format(len(rows)))
                    for row in rows:
                        anEvent = "input_name=\"{0}\" tracking_uuid=\"{1}\" is_data_golden=\"{2}\" ".format(
                            self.get_config("name").replace("ga://", ""), run_uuid, is_data_golden)
                        anEvent += " raw_row=\"{}\" ".format(dumps(row))
                        dimensions = row.get('dimensions', [])
                        dateRangeValues = row.get('metrics', [])

                        explicit_time = None
                        for header, dimension in zip(dimensionHeaders, dimensions):
                            dimension_name = header.replace(":", "_dimension_")
                            if "time_dimension" in kwargs:
                                if dimension_name == kwargs["time_dimension"].replace(":", "_dimension_"):
                                    import re
                                    m = re.match(
                                        r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d+)T(?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)\.(?P<ms>\d+)(?P<tz>.+)",
                                        dimension)

                                    tzhours, tzminutes = m.group('tz').split(":")
                                    tztd = dt.timedelta(seconds=(int(tzhours) * 3600 + int(tzminutes) * 60))
                                    base_datetime = dt.datetime(int(m.group("year")), int(m.group("month")),
                                                                int(m.group("day")),
                                                                int(m.group("hour")), int(m.group("minute")),
                                                                int(m.group("second")),
                                                                int(m.group("ms")))
                                    explicit_time = time.mktime((base_datetime + tztd).timetuple())
                            anEvent += self._sanitize(dimension_name) + '="' + self._sanitize(dimension) + '" '

                        self.log.debug("{}".format(dumps(dateRangeValues)))
                        for i, values in enumerate(dateRangeValues):
                            anEvent += 'date_range="' + str(i) + '" '
                            for metricHeader, value in zip(metricHeaders, values.get('values')):
                                anEvent += self._sanitize(
                                    metricHeader.get('name').replace(":", "_metric_")) + '="' + self._sanitize(
                                    value) + '" '
                        self.log.debug("event line:: {}".format(anEvent))
                        self.print_event(''.join(filter(lambda qt: qt in string.printable, anEvent)),
                                         explicit_time=explicit_time)
            return selected_dates
        except Exception as e:
            self._catch_error(e)
            return []

    ## CLASSROOM API
    def threaded_classroom_report(self, report, **kwargs):
        try:
            self_functions = {
                "courses:aliases": self.courses_aliases,
                "courses:submit": self.courses_submit,
                "courses:topics": self.courses_topics,
                "classroom:invitations": self.classroom_invitations
            }
            self.log.debug("report={} kwargs={}".format(report, kwargs))
            if report not in self_functions:
                self.log.warn("report={} error=not_found".format(report))
            return self_functions[report](course=kwargs.get("course"))
        except Exception as e:
            self.log.warn("report={} error={} courses={}".format(report, e, kwargs["courses"]))
            self._catch_error(e)

    def courses_aliases(self, course):
        service = build('classroom', 'v1', http=self.http_session)
        return self.classroom_paginator(service.courses().aliases(), "aliases", course)

    def courses_submit(self, course):
        return course

    def courses_topics(self, course):
        return course

    def classroom_invitations(self, course):
        return course

    def classroom_paginator(self, api_caller, objecti, course):
        try:
            self.log.info("status=starting objecti={}".format(objecti))
            self.sourcetype("gapps:classroom:courses:{}".format(objecti))
            page_token = None
            local_count = 0
            total_count = 0
            error_found = False
            params = {"courseId": course}
            while True and not error_found:
                try:
                    if page_token:
                        params['pageToken'] = page_token
                    self.debug("page_token={}".format(page_token))
                    current_page = api_caller.list(**params).execute()
                    self.debug("got_current_page")
                    if objecti in current_page:
                        self.print_multiple_events([x for x in current_page[objecti]])
                        total_count = total_count + len(current_page[objecti])
                        local_count = local_count + len(current_page[objecti])
                    page_token = current_page.get('nextPageToken')
                    if not page_token:
                        break
                except errors.HttpError as error:
                    self.log.warn("action=no_data_found")
                    break
                except Exception as e:
                    self.log.warn("action=caught_admin_error error_type={} error={}".format(type(e), e))
                    error_found = True
                    break
        except Exception as e:
            self._catch_error(e)

    def courses(self, write_courses=False):
        try:
            function_name = "courses"
            self.info("function={} status=starting write_courses={}".format(function_name, write_courses))
            self.sourcetype("gapps:classroom:courses")
            service = build('classroom', 'v1', http=self.http_session)
            page_token = None
            local_count = 0
            total_count = 0
            error_found = False
            params = {}
            course_ids = []

            def add_course_id(course):
                course_ids.append(course["id"])
                return course
            while True and not error_found:
                try:
                    if page_token:
                        params['pageToken'] = page_token
                    self.debug("function={} page_token={}".format(function_name, page_token))
                    current_page = service.courses().list(**params).execute()
                    self.debug("function={} got_current_page".format(function_name))
                    if "courses" in current_page:
                        writeThem = [add_course_id(x) for x in current_page["courses"]]
                        if write_courses:
                            self.print_multiple_events(writeThem, time_field="updateTime")
                        total_count = total_count + len(current_page["courses"])
                        local_count = local_count + len(current_page["courses"])
                    page_token = current_page.get('nextPageToken')
                    if not page_token:
                        break
                except errors.HttpError as error:
                    self.log.warn("function={} action=no_data_found".format(function_name))
                    break
                except Exception as e:
                    self.log.warn("function={} action=caught_admin_error error_type={} error={}".format(function_name, type(e), e))
                    error_found = True
                    break
            return course_ids
        except Exception as e:
            self._catch_error(e)

    ## DIRECTORY API
    def admin_directory_users(self):
        try:
            self.info("function=admin_directory_users status=starting")
            self.sourcetype("gapps:admin:directory:{}".format("users"))
            service = build('admin', 'directory_v1', http=self.http_session)
            self.debug("built the service")
            page_token = None
            local_count = 0
            total_count = 0
            error_found = False
            params = {"customer": "my_customer", "orderBy": "email", "viewType": "admin_view"}
            while True and not error_found:
                try:
                    if page_token:
                        params['pageToken'] = page_token
                    self.debug("Have A Page? :%s" % page_token)
                    current_page = service.users().list(**params).execute()
                    self.debug("got a current_page")
                    if "users" in current_page:
                        self.print_multiple_events([x for x in current_page["users"]])
                        total_count = total_count + len(current_page["users"])
                        local_count = local_count + len(current_page["users"])
                    page_token = current_page.get('nextPageToken')
                    if not page_token:
                        break
                except errors.HttpError as error:
                    self.log.info("action=no_data_found")
                    error_found = True
                    break
                except Exception as e:
                    self.log.debug("action=caught_admin_error error_type={} error={}".format(type(e), e))
                    self._catch_error(e)
                    error_found = True
            return True
        except Exception as e:
            self._catch_error(e)

    def usage_customer_report(self, start_date):
        try:
            self.info("function=adminReportCustomerUsage status=starting")
            service = build('admin', 'reports_v1', http=self.http_session)
            self.debug("built the service")
            # Set start time to one week ago, to avoid too many results
            # 2015-04-23T14:24:48.802688Z
            self.debug("checkpoint: {}".format(start_date))
            start_time = datetime.strptime(start_date, "%Y-%m-%d")
            self.debug("start_time: {}".format(start_time))
            rightNow = datetime.utcnow()
            self.debug("rightnow: %s" % rightNow)
            numDays = (rightNow - start_time).days
            self.debug("checkpoint: %s  rightNow:%s numDays:%s " % (start_time, rightNow, numDays))
            if numDays < 1:
                self.info("operation=checkpoint_check numberOfDays=%s checkpoint=%s execution_time=%s" % (
                    numDays, start_time, rightNow))
                return
            reportDates = [d.strftime("%Y-%m-%d") for d in [rightNow - timedelta(days=x) for x in range(0, numDays)]]
            self.debug("action=usage_user_reports dates_to_check={}".format(reportDates))
            myEvents = []
            total_count = 0
            for myDate in reportDates:
                page_token = None
                params = {'date': myDate}
                self.info("operation=starting_while_loop_for_pages date=%s" % myDate)
                local_count = 0
                error_found = False
                while True:
                    try:
                        if page_token:
                            params['pageToken'] = page_token
                        self.debug("Have A Page? :%s" % page_token)
                        current_page = service.customerUsageReports().get(**params).execute()
                        self.debug("got a current_page")
                        if "usageReports" in current_page:
                            self.print_multiple_events([self._usage_fix(x) for x in current_page["usageReports"]])
                            total_count = total_count + len(current_page["usageReports"])
                            local_count = local_count + len(current_page["usageReports"])
                        page_token = current_page.get('nextPageToken')
                        if not page_token:
                            break
                    except errors.HttpError as error:
                        self.log.info("action=no_data_found date={}".format(myDate))
                        error_found = True
                        break
                    except Exception as e:
                        self.log.debug("action=caught_usage_error error_type={} error={}".format(type(e), e))
                        self._catch_error(e)
                self.debug("Have events, will travel: %s" % len(myEvents))
                if not error_found:
                    myEvents.extend([{"date": myDate, "total_count": local_count}])
            return myEvents
        except Exception as e:
            self._catch_error(e)

    @contextlib.contextmanager
    def nostdout(self):
        save_stdout = sys.stdout
        sys.stdout = io.BytesIO()
        yield
        sys.stdout = save_stdout

    def usage_user_report(self, start_date):
        try:
            # TODO: Write a <done> event for every days worth of data.
            self.info("action=usage_user function=adminReportUserUsage status=starting")
            service = build('admin', 'reports_v1', http=self.http_session)
            self.debug("action=usage_user built the service")
            # Set start time to one week ago, to avoid too many results
            # 2015-04-23T14:24:48.802688Z
            self.debug("action=usage_user checkpoint={}".format(start_date))
            start_time = datetime.strptime(start_date, "%Y-%m-%d")
            self.debug("action=usage_user start_time={}".format(start_time))
            rightNow = datetime.utcnow()
            numDays = (rightNow - start_time).days
            self.debug("action=usage_user action=usage_pull checkpoint={} rightNow={} numDays={} ".format(start_time, rightNow, numDays))
            if numDays < 1:
                self.info("action=usage_user operation=checkpoint_check numberOfDays={} checkpoint={} execution_time={}".format(
                    numDays, start_time, rightNow))
                return
            reportDates = [d.strftime("%Y-%m-%d") for d in [rightNow - timedelta(days=x) for x in range(0, numDays)]]
            self.debug("action=usage_user action=usage_user_reports dates_to_check={}".format(reportDates))
            myEvents = []
            total_count = 0
            for myDate in reportDates:
                page_token = None
                params = {'userKey': 'all', 'date': myDate}
                self.info("action=usage_user operation=starting_while_loop_for_pages date=%s" % myDate)
                local_count = 0
                error_found = False
                while True:
                    try:
                        if page_token:
                            params['pageToken'] = page_token
                        self.debug("action=usage_user Have A Page? :%s" % page_token)
                        current_page = service.userUsageReport().get(**params).execute()
                        self.debug("action=usage_user got a current_page")
                        if "usageReports" in current_page:
                            self.print_multiple_events([self._usage_fix(x) for x in current_page["usageReports"]])
                            total_count = total_count + len(current_page["usageReports"])
                            local_count = local_count + len(current_page["usageReports"])
                        page_token = current_page.get('nextPageToken')
                        if not page_token:
                            break
                    except errors.HttpError as error:
                        self.log.info("action=usage_user action=no_data_found date={}".format(myDate))
                        error_found = True
                        break
                    except Exception as e:
                        self.log.debug("action=usage_user action=caught_usage_error error_type={} error={}".format(type(error), error))
                        self._catch_error(e)
                self.debug("action=usage_user Have events, will travel: %s" % len(myEvents))
                if not error_found:
                    myEvents.extend([{"date": myDate, "total_count": local_count}])
            return myEvents
        except Exception as e:
            self._catch_error(e)

    def _usage_fix(self, evt):
        evt["timestamp"] = "{} 00:00:00".format(evt["date"])
        for param in evt["parameters"]:
            if "name" in param:
                k = param["name"].replace(":", "_")
                v = "NO_TYPE_FOUND"
                if "intValue" in param:
                    v = param["intValue"]
                elif "boolValue" in param:
                    v = param["boolValue"]
                elif "stringValue" in param:
                    v = param["stringValue"]
                elif "datetimeValue" in param:
                    v = param["datetimeValue"]
                param[k] = v
        return evt

    def is_integer(self, s):
        try:
            int(s)
            return True
        except ValueError:
            return False

    def gapps_admin_sdk_reports(self, **kwargs):
        chkpoint = kwargs["checkpoint"]
        application_name = kwargs["applicationName"]
        self.info(
            "function=adminReportV1 status=starting application_name={} time_of_gmtime={} timezone_of_localtime={}".format(
                application_name, time.strftime("%Y-%m-%dT%H:%M:%S%z  %Z", time.gmtime()),
                time.strftime("%Y-%m-%dT%H:%M:%S%z %Z", time.localtime())))
        service = build('admin', 'reports_v1', http=self.http_session)
        interval = 3600
        kwargs_interval = kwargs["interval"]
        self.info("operation=check_interval type={} is_integer={} interval={}".format(type(kwargs_interval),
                                                                                      self.is_integer(kwargs_interval),
                                                                                      kwargs_interval))
        if isinstance(kwargs_interval, int) or self.is_integer(kwargs_interval):
            self.info("operation=set_interval type=int interval={}".format(kwargs_interval))
            interval = int(kwargs_interval)
        else:
            self.warn("operation=set_interval type={} interval={}".format(type(kwargs_interval), kwargs_interval))
        start_time = time.strftime("%Y-%m-%dT%H:%M:%S", (time.localtime(chkpoint - interval)))
        # ASA-211 TZ parsing is weird. Google must either have "Z" or [+-]\d\d:\d\d neither of which is native to python
        # ASA-248 Problems with strftime and localtime in Python causes issues in data collection
        my_local_tz = time.strftime("%z")
        last_two = my_local_tz[-2:]
        first_two = my_local_tz[:-2]
        myTZ = "{}:{}".format(first_two, last_two)
        start_time = "{}{}".format(start_time, myTZ)
        self.info("operation=setting_api_constraints_time special={} application_name={}".format(start_time,
                                                                                                 application_name))
        wibbly_wobbly_timey_wimey = datetime.utcnow() - timedelta(seconds=interval)
        # Why did I hard code a timezone? ffs, let's do a tz calculation from local -> utc.
        end_time = time.strftime("%Y-%m-%dT%H:%M:%S",
                                 time.localtime(
                                     (wibbly_wobbly_timey_wimey - datetime.utcfromtimestamp(0)).total_seconds()))
        end_time = "{}{}".format(end_time, myTZ)
        # ASA-9 : Hard Coded 2 Hour Buffer to the Checkpoint
        #  This allows Google to correlate and condense their information and return correctly.
        # time.strftime("%Y-%m-%dT%H:%M:%SZ", chkpoint))
        self.info(
            "operation=setting_api_constraints_time start_time={} end_time={} application_name={}".format(start_time,
                                                                                                          end_time,
                                                                                                          application_name))
        total_count = 0
        page_token = None
        params = {'applicationName': application_name, 'userKey': 'all', 'startTime': start_time, 'endTime': end_time}
        self.info("operation=starting_while_loop_for_pages application_name={}".format(application_name))
        while True:
            try:
                if page_token:
                    params['pageToken'] = page_token
                self.debug("operation=has_page application_name={} token={}".format(application_name, page_token))
                self.debug("operation=has_page application_name={} param_type={} params={}".format(application_name,
                                                                                                   type(params),
                                                                                                   params))
                self.debug("credential={}".format(self.credentials))
                current_page = service.activities().list(**params).execute()
                self.debug(
                    "operation=has_page application_name={} current_page={}".format(application_name, current_page))
                if "items" in current_page:
                    self.debug("operation=has_items_in_page")
                    self.print_multiple_events([self.process_admin_api_evts(x, application_name)
                                                for x in current_page["items"]], time_field="time")
                    # NOTICE THAT "time" is not valid. it is "id.time", but will need to refactor to fix.
                    self.end_stream()
                    self.init_stream()
                    total_count += total_count + len(current_page["items"])
                page_token = current_page.get('nextPageToken')
                if not page_token:
                    break
            except Exception as e:
                self.log.error("error={}".format(e))
                self._catch_error(e)
                break
        self.info("action=found_events num_events={0} application_name={1} start_time=\"{2}\" end_time=\"{3}\"".format(
            total_count, application_name, start_time, end_time))
        try:
            oldst = self.sourcetype()
            self.sourcetype("{}:modular_input_result".format(oldst))
            self.print_multiple_events([{"application_name": application_name, "start_time": start_time,
                                         "end_time": end_time, "num_events": total_count}])
            self.sourcetype(oldst)
        except Exception as e:
            self.log.error("error={}".format(e))
            self._catch_error(e)

    def process_admin_api_evts(self, evt, application_name):
        if "events" in evt:
            for event in evt["events"]:
                if "parameters" in event:
                    for param in event["parameters"]:
                        if "value" in param:
                            param[param["name"]] = param["value"]
                        elif "intValue" in param:
                            param[param["name"]] = param["intValue"]
                        elif "multiValue" in param:
                            param[param["name"]] = param["multiValue"]
                        elif "boolValue" in param:
                            param[param["name"]] = param["boolValue"]
                        else:
                            param[param["name"]] = "NO VALUE DETECTED"
        return evt

    def get_usage_chrome_os_devices(self, **kwargs):
        try:
            # TODO: Write a <done> event for every days worth of data.
            self.info("function=chrome_os_devices_usage status=starting")
            service = build('admin', 'directory_v1', http=self.http_session)
            self.debug("built the service")
            page_token = None
            total_count = 0
            params = {'customerId': 'my_customer', 'orderBy': "status", 'projection': "FULL"}
            self.info("operation=starting_while_loop_for_pages")
            while True:
                try:
                    if page_token:
                        params['pageToken'] = page_token
                    self.debug("Have A Page? :%s" % page_token)
                    current_page = service.chromeosdevices().list(**params).execute()
                    self.debug("got a current_page")
                    if "chromeosdevices" in current_page:
                        self.print_multiple_events([x for x in current_page.get('chromeosdevices')])
                        total_count = total_count + len(current_page.get('chromeosdevices'))
                    page_token = current_page.get('nextPageToken')
                    if not page_token:
                        break
                except errors.HttpError as e:
                    self._catch_error("error=http_error message='{}'".format(e))
                    break
                except TypeError as e:
                    self._catch_error("error=type_error message={}".format(e))
                    break
                except Exception as e:
                    self._catch_error(e)
            self.sourcetype("gapps:chrome:api")
            self.print_event(dumps({"total_count": total_count}))
            # No need to do usage fix.
        except Exception as e:
            self._catch_error(e)

    def get_alert_center_alerts(self, **kwargs):
        chkpoint = kwargs["checkpoint"]
        source = kwargs["source"]
        self.info(
            "status=starting source={} time_of_gmtime={} timezone_of_localtime={}".format(
                source, time.strftime("%Y-%m-%dT%H:%M:%S%z  %Z", time.gmtime()),
                time.strftime("%Y-%m-%dT%H:%M:%S%z %Z", time.localtime())))
        service = build('alertcenter', 'v1beta1', http=self.http_session)
        interval = 3600
        kwargs_interval = kwargs["interval"]
        self.info("operation=check_interval type={} is_integer={} interval={}".format(type(kwargs_interval),
                                                                                      self.is_integer(kwargs_interval),
                                                                                      kwargs_interval))
        if isinstance(kwargs_interval, int) or self.is_integer(kwargs_interval):
            self.info("operation=set_interval type=int interval={}".format(kwargs_interval))
            interval = int(kwargs_interval)

        else:
            self.warn("operation=set_interval type={} interval={}".format(type(kwargs_interval), kwargs_interval))
        start_time = time.strftime("%Y-%m-%dT%H:%M:%S", (time.localtime(chkpoint - interval)))
        # ASA-211 TZ parsing is weird. Google must either have "Z" or [+-]\d\d:\d\d neither of which is native to python
        # ASA-248 Problems with strftime and localtime in Python causes issues in data collection
        my_local_tz = time.strftime("%z")
        last_two = my_local_tz[-2:]
        first_two = my_local_tz[:-2]
        myTZ = "{}:{}".format(first_two, last_two)
        start_time = "{}{}".format(start_time, myTZ)
        self.info("operation=setting_api_constraints_time special={} source={}".format(start_time,
                                                                                       source))
        self.info(
            "operation=setting_api_constraints_time start_time={} source={}".format(start_time, source))
        total_count = 0
        page_token = None
        # createTime >=
        # https://developers.google.com/admin-sdk/alertcenter/guides/query-filters
        filter_items = ['createTime>={}'.format(start_time)]
        alert_transpose = {"token": "Domain wide takeout",
                           "gmail": "Gmail phishing",
                           "identity": "Google identity",
                           "operations": "Google Operations",
                           "state": "State Sponsored Attack",
                           "mobile": "Mobile device management"}
        if source != "all":
            filter_items.append('source="{}"'.format(alert_transpose[source]))
        params = {'filter': " AND ".join(filter_items)}
        self.info("operation=starting_while_loop_for_pages source={}".format(source))
        while True:
            try:
                if page_token:
                    params['pageToken'] = page_token
                self.debug("operation=has_page source={} token={}".format(source, page_token))
                self.debug("operation=has_page source={} param_type={} params={}".format(source,
                                                                                         type(params),
                                                                                         params))
                current_page = service.alerts().list(**params).execute()
                self.debug(
                    "operation=has_page source={} current_page={}".format(source, current_page))
                if "alerts" in current_page:
                    self.debug("operation=has_items_in_page")
                    self.print_multiple_events([self.process_alert_api_evts(x, source)
                                                for x in current_page["alerts"]], time_field="createTime")
                    # NOTICE THAT "time" is not valid. it is "id.time", but will need to refactor to fix.
                    self.end_stream()
                    self.init_stream()
                    total_count += total_count + len(current_page["alerts"])
                page_token = current_page.get('nextPageToken')
                if not page_token:
                    break
            except Exception as e:
                self._catch_error(e)
                break

        self.info("action=found_events num_events={0} source={1} start_time=\"{2}\" ".format(
            total_count, source, start_time))

    def get_drive_information(self, fileId, fields="*"):
        try:
            # GET https://www.googleapis.com/drive/v3/files/fileId
            self.info("function=get_drive_information status=starting ")
            service = build('drive', 'v3', http=self.http_session)
            self.info("operation=starting_while_loop_for_pages")
            try:
                file_metadata = service.files().get(fileId=fileId, fields=fields).execute()
            except Exception as e:
                file_metadata = "{}".format(e)
            return file_metadata
        except Exception as e:
            self._catch_error(e)

    def get_gmail_oauth_credentials(self):
        try:
            from oauth2client.service_account import ServiceAccountCredentials
            return ServiceAccountCredentials.from_json_keyfile_name(
                os.path.join(self._app_local_directory,
                             "GoogleApps." + self.get_config("domain").lower() + ".gmail.cred"),
                scopes=['https://www.googleapis.com/auth/admin.reports.audit.readonly',  # ADMIN SDK REPORTS
                        'https://www.googleapis.com/auth/admin.reports.usage.readonly',  # ADMIN SDK USAGE
                        'https://www.googleapis.com/auth/analytics.readonly',  # Google Analytics API
                        'https://www.googleapis.com/auth/gmail.readonly',  # Allows for Gmail reading.
                        'https://www.googleapis.com/auth/admin.directory.user.readonly'
                        # Allows Directory Reading.
                        ])
        except Exception as e:
            self.print_error("Getting OAUTH Credentials Failed: %s" % e)
            sys.exit(1)

    def gapps_gmail(self, **kwargs):
        # REF https://developers.google.com/identity/protocols/OAuth2ServiceAccount#delegatingauthority
        chkpoint = kwargs["checkpoint"]
        # application_name = kwargs["applicationName"]
        self.info("function=gmailV1 status=starting ")
        user = "meghan@kyleasmith.info"
        gmail_credentials = self.get_gmail_oauth_credentials()
        # delegated_creds = gmail_credentials.create_delegated(user)
        self.http_session = gmail_credentials.authorize(
            httplib2.Http(proxy_info=self._get_proxy_info(self._app_local_directory)))
        service = build('gmail', 'v1', http=self.http_session)

        results = service.users().labels().list(userId='meghan@kyleasmith.info').execute()
        self.log.info("results of GMAIL: {}".format(results))
        print("{}".format(results))

    def set_local_directory(self, ld):
        self._app_local_directory = ld

    def set_bin_directory(self, ld):
        self._app_bin_directory = ld

    def _get_proxy_info(self, _app_local_directory):
        proxy_config_file = os.path.join(_app_local_directory, "proxy.conf")
        proxy_info = None
        utils = Utilities(app_name=self._app_name, session_key=self.get_config("session_key"))
        if os.path.isfile(proxy_config_file):
            try:
                pc = utils.get_proxy_configuration("gapps_proxy")
                scheme = "http"
                if pc["useSSL"] == "true":
                    scheme = "https"
                self.log.debug("action=setting_proxy scheme={} host={}, port={} username={}".format(scheme,
                                                                                                  pc["host"],
                                                                                                  pc["port"],
                                                                                                  pc["authentication"][
                                                                                                      "username"]))
                if pc["authentication"]["username"]:
                    proxy_url = "{}://{}:{}@{}:{}/".format(scheme, pc["authentication"]["username"],
                                                           pc["authentication"]["password"], pc["host"], pc["port"])
                else:
                    proxy_url = "{}://{}:{}/".format(scheme, pc["host"], pc["port"])
                proxy_info = {"http": proxy_url, "https": proxy_url}
            except Exception as e:
                self.log.warn("action=load_proxy status=failed message=No_Proxy_Information stanza=gapps_proxy")
        return proxy_info

    def setup_bigquery_session(self, oauth_credentials, app_local_dir, project):
        # http = httplib2.Http(proxy_info=self._get_proxy_info(app_local_dir))
        # self.credentials = oauth2client.client.OAuth2Credentials.from_json(dumps(oauth_credentials))
        # self.bq_http = http
        # self._app_local_directory = app_local_dir
        # self.bq_http_session = self.credentials.authorize(self.bq_http)
        # self.bq_client = bigquery.client.Client(project=project,
        # _http=self.bq_http_session)
        try:
            self.info("action=starting_bigquery app_local_dir={} project={}".format(app_local_dir, project))
            import pkg_resources
            import importlib
            importlib.reload(pkg_resources)
            pkg_resources.get_distribution('google-cloud-bigquery')
            import six
            self.info("path={}".format(sys.path))
            from google.cloud import bigquery
            from google.oauth2 import service_account
            self.credentials = service_account.Credentials.from_service_account_info(oauth_credentials)
            self.info("credential_type={}".format(type(self.credentials)))
            self._app_local_directory = app_local_dir
            self.bq_client = bigquery.Client(project=project, credentials=self.credentials)
        except Exception as e:
            import traceback
            myJson = {"timestamp": self.gen_date_string(), "log_level": "ERROR"}
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            myJson["errors"] = [{"msg": str(e),
                                 "exception_type": "{}".format(type(e)),
                                 "exception_arguments": "%s" % e,
                                 "filename": fname,
                                 "exception_line": exc_tb.tb_lineno,
                                 "input_name": self.get_config("name"),
                                 "traceback": traceback.format_exc()
                                 }]
            self.log.error("{}".format(myJson))
            self._catch_error(e)
            return myJson

    def setup_http_session(self, oauth_credentials, app_local_dir):
        http = httplib2.Http(proxy_info=self._get_proxy_info(app_local_dir))
        self.credentials = google.oauth2.credentials.Credentials(**oauth_credentials)
        self.http_session = http
        self._app_local_directory = app_local_dir
        self.log.debug("action=setup_http_session step=authorize_http")
        try:
            self.log.info("action=removed")
            self.http_session = google_auth_httplib2.AuthorizedHttp(self.credentials, http = http)
            self.log.info("action=setup_httplib2 message=if_you_are_reading_this_good_luck_and_godspeed")
        except Exception as e:
            self.log.error("Error on Authorize: {}".format(e))

    def _process_bq_row(self, record):
        self.print_event("{}".format(record))
        #     for field_name in field_names:
        #         record[field_name] = row[field_name]
        #     i += 1
        #     event = helper.new_event(time=get_timestamp(get_time(helper.get_arg('timestamp_column_name'), record),
        #                                                 helper.get_arg('timestamp_format')), source=source,
        #                              index=helper.get_output_index(), sourcetype=sourcetype,
        #                              data=dumps(record, default=str))
        #     ew.write_event(event)

    def bigquery_ingest_all_tables(self, project, dataset):
        """
        Using the checkpoint for the project/dataset, keep track of 1) table name, and 2) last index.
        If "table.num_rows" > last_index, then consume from there. for now, do this for all tables.
        It's not scalable, but I can fix that later.
        :param project:
        :param dataset:
        :return:
        """
        try:
            self.log.info("action=starting project={} dataset={}".format(project, dataset))
            dataset_ref = self.bq_client.dataset(dataset, project=project)
            self.log.info("action=get_dataset_ref ref={}".format(dataset_ref))
            tables = [self._bigquery_table_information(project, dataset, t.table_id) for t in
                      self.bq_client.list_tables(dataset_ref)]
            self.log.debug("tables={}".format(tables))
            self.source("gapps:bigquery:{}:tables".format(dataset))
            self.sourcetype("gapps:bigquery:table:fields".format(project))
            [self.bigquery_query_all_fields_by_table(project, dataset, t.table_id) for t in tables]
            self.print_multiple_events(
                [{"table_id": t.table_id, "num_rows": t.num_rows, "schema": "{!s:<16}".format(t.schema)} for t in
                 tables])
            return tables
        except Exception as e:
            self._catch_error(e)

    def _bigquery_table_information(self, project, dataset, table):
        dataset_ref = self.bq_client.dataset(dataset, project=project)
        table_ref = dataset_ref.table(table)
        table = self.bq_client.get_table(table_ref)
        # Get Checkpoint, setting execution times and last calls
        self.source("gapps:bigquery:{}:table:{}".format(dataset, table.table_id))
        self.sourcetype("gapps:bigquery:table".format(project))
        self.print_multiple_events([{"table_information": "{}".format(table.__dict__)}])
        return table

    def _load_bq_table_checkpoint(self, dataset, table):
        _chkpoint_key = "{}_{}_{}".format(self.get_config("domain"), "bigquery", dataset)
        _chkpoint = self._get_checkpoint(_chkpoint_key)
        self.log.debug("checkpoint found_checkpoint {}".format(_chkpoint))
        if _chkpoint is None:
            self.log.debug("checkpoint checkpoint None")
            _chkpoint = {"execution_time": 0, "completed_days": [], "tables": {}}
            self._set_checkpoint(_chkpoint_key, _chkpoint)
            self.log.debug("checkpoint checkpoint set {}".format(_chkpoint))
        if "tables" not in _chkpoint:
            _chkpoint["tables"] = {}
        self.log.info("type=configured_checkpoint checkpoint={}".format(_chkpoint))
        default_table_checkpoint = {"id": table.table_id, "num_rows": table.num_rows, "last_row": 0}
        self.log.debug("type=setting_default_checkpoint checkpoint={}".format(default_table_checkpoint))
        return _chkpoint["tables"].get(table.table_id, default_table_checkpoint)

    def _set_bq_table_checkpoint(self, dataset, table, data):
        _chkpoint_key = "{}_{}_{}".format(self.get_config("domain"), "bigquery", dataset)
        current_checkpoint = self._get_checkpoint(_chkpoint_key)
        current_checkpoint["tables"][table.table_id] = data
        self._set_checkpoint(_chkpoint_key, current_checkpoint)

    def _check_cell_values(self, cell_value):
        cell_type = type(cell_value)
        # self.log.info("cell_type={} cell_value={!s:<16}".format(cell_type, cell_value))
        if isinstance(cell_value, dict):
            return cell_value
        return "{!s:<16}".format(cell_value)

    def do_single_row(self, row, field_names, time_field):
        row_object = {field_names[i]: self._check_cell_values(row[i]) for i in range(len(row))}
        tf = time_field
        if time_field == "event_info":
            tf = "timestamp"
            try:
                row_object["timestamp"] = int(
                    "{}".format(row_object.get("event_info", {}).get("timestamp_usec", 1000000))) / 1000000
            except Exception as e:
                self.log.error("action=set_time_field tf={} has_event_info={} has_timestamp_usec={}".format(tf,
                                                                                                            "event_info" in row_object,
                                                                                                            "timestamp_usec" in row_object.get(
                                                                                                                "event_info",
                                                                                                                {})))
                self._catch_error(e)
        self.log.info("action=set_time_field  tf={} value={}".format(tf, row_object.get("timestamp", "NotAvail")))
        self.print_multiple_events([row_object])
        self.print_done_event()
        self.end_stream()
        self.init_stream()
        return True

    def bigquery_query_all_fields_by_table(self, project, dataset, atable, **kwargs):
        try:
            self.sourcetype("gapps:bigquery:table")
            self.source("gapps:bigquery:{}:{}:{}".format(project, dataset, atable))
            self.log.info("action=begin project={} dataset={} table={}".format(project, dataset, atable))
            table = self._bigquery_table_information(project, dataset, atable)  # API call
            _chkpoint = self._load_bq_table_checkpoint(dataset, table)
            if table.num_rows <= _chkpoint.get("last_row") or table.num_rows == 0:
                self.log.info("action=stop_execution project={} dataset={} table={} last_row={} num_rows={}".format(
                    project, dataset, atable, _chkpoint.get("last_row"), table.num_rows
                ))
                return
            self.log.info("action=did_not_stop_execution project={} dataset={} table={} last_row={} num_rows={}".format(
                project, dataset, atable, _chkpoint.get("last_row"), table.num_rows
            ))
            fields = table.schema[:]
            extra_params = {"selected_fields": fields}
            if "start_index" in kwargs:
                extra_params["start_index"] = kwargs["start_index"]
            else:
                extra_params["start_index"] = _chkpoint.get("last_row")
            if "selected_fields" in kwargs:
                extra_params["selected_fields"] = kwargs["selected_fields"]
            rows = self.bq_client.list_rows(table, **extra_params)
            field_names = [field.name for field in rows.schema]
            time_field = "timestamp"
            # Find the first occurance of a timestamp field. If none, it becomes ingest time.
            for field in rows.schema:
                self.log.info("action=check_for_time field_name={} field_type={}".format(field.name, field.field_type))
                if "{}".format(field.field_type) == "TIMESTAMP":
                    time_field = field.name
                    break
                if "{}".format(field.name) == "event_info":
                    time_field = "event_info"
            self.log.info("action=set_time_field time_field={}".format(time_field))
            self.sourcetype("gapps:bigquery:table:data")
            print_rows = [self.do_single_row(row, field_names, time_field) for row in rows]
            self.sourcetype("gapps:bigquery:table")
            row_count = len(print_rows)
            last_row_index = int(extra_params["start_index"]) + row_count
            _chkpoint["last_row"] = last_row_index
            self.log.info(
                "action=setting_checkpoint project={} dataset={} table={} last_row={} num_rows={} added_rows={}".format(
                    project, dataset, atable, _chkpoint.get("last_row"), table.num_rows, row_count
                ))
            self._set_bq_table_checkpoint(dataset, table, _chkpoint)
        except Exception as e:
            self._catch_error(e)

    def _validate_arguments(self, val_data):
        return True
        val_data = self._get_validation_data()
        try:
            serviceName = val_data["servicename"].split(':')
            valkey = serviceName[0]
            valval = serviceName[1]
            self.info("operation=validating input key=%s value=%s" % (valkey, valval))
            if valval not in MI.available_apis[valkey]:
                raise Exception("No Valid Report Key set. Failed excuse was key %s with value %s" % (valkey, valval))
        except IOError as e:
            self.print_error("OAuth file not found. Did you run the run_first.py? %s" % (str(e)))
            sys.exit(_SYS_EXIT_FAILED_VALIDATION)
        except Exception as e:
            self.print_error("Invalid configuration specified: {}".format(e))
            sys.exit(_SYS_EXIT_FAILED_VALIDATION)

    def get_spreadsheets(self, spreadsheetId=None, includeGridData=False):
        try:
            self.log.info("action=get_spreadsheets id={} data_include={}".format(spreadsheetId, includeGridData))
            ss = build('sheets', 'v4', http=self.http_session)
            return ss.spreadsheets().get(spreadsheetId=spreadsheetId, includeGridData=includeGridData).execute()
        except Exception as e:
            self.print_error("Get Spreadsheets Error: {}".format(e))
            self._catch_error(e)

    def parse_row(self, hr, md, sh, ridx):
        try:
            obj = {"metadata": md, "fields": {}, "ordered_fields": {}, "row_index": ridx}
            if sh is None:
                return obj
            if sh.get("values") is None:
                return obj
            for idx, it in enumerate(sh.get("values")):
                field_name = "COLUMN_{}".format("{}".format(idx).rjust(5, '0'))
                field_value = it.get("formattedValue", "")
                obj["ordered_fields"][field_name] = field_value
                try:
                    field_name = hr[idx].replace(" ", "_")
                except IndexError:
                    pass
                obj["fields"][field_name] = field_value
            return obj
        except Exception as e:
            self.catch_error(e)

    def parse_spreadsheet_data(self, ss, sheet_information, destination=False):
        try:
            self.info("ss={} sheet={}".format(ss, sheet_information))
            sheet_data = sheet_information.get("data", {})[0].get("rowData", [])
            hr = []
            for index, item in enumerate(sheet_data[0].get("values")):
                hr.append(item.get("formattedValue", "COLUMN_{}".format("{}".format(index).rjust(5, '0'))))
            metadata = {"spreadsheet_title": ss.get("title", "not_available"),
                        "spreadsheet_id": ss.get("id", "not_available"),
                        "sheet_properties": sheet_information.get("properties"),
                        "header_row": hr}
            self.info("header_row={}".format(dumps(hr)))
            return [self.parse_row(hr, metadata, sheet, idx) for idx, sheet in enumerate(sheet_data)]
        except Exception as e:
            self.catch_error(e)
