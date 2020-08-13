import sys
from splunk.appserver.mrsparkle.lib.util import make_splunkhome_path
_APP_NAME = 'GSuiteForSplunk'
import os.path

sys.path.insert(0, make_splunkhome_path(["etc", "apps", _APP_NAME, "bin", "lib"]))
sys.path.insert(0, make_splunkhome_path(["etc", "apps", _APP_NAME, "bin", "lib", "python3.7", "site-packages"]))

# https://support.google.com/a/answer/7061566
import logging as log
import time
from datetime import timedelta, datetime
import json
import splunk.appserver.mrsparkle.lib.util as util
from requests.exceptions import *

from splunk.appserver.mrsparkle.lib.util import isCloud

from GoogleAppsForSplunkModularInput import GoogleAppsForSplunkModularInput
from Utilities import KennyLoggins, Utilities

__author__ = 'ksmith'

_MI_APP_NAME = 'G Suite For Splunk Modular Input'
_APP_NAME = 'GSuiteForSplunk'
# SYSTEM EXIT CODES
_SYS_EXIT_FAILED_VALIDATION = 7
_SYS_EXIT_FAILED_GET_OAUTH_CREDENTIALS = 6
_SYS_EXIT_FAILURE_FIND_API = 5
_SYS_EXIT_OAUTH_FAILURE = 4
_SYS_EXIT_FAILED_CONFIG = 3

# Necessary
_CRED = None
_DOMAIN = None

_SPLUNK_HOME = os.getenv("SPLUNK_HOME")
if _SPLUNK_HOME is None:
    _SPLUNK_HOME = make_splunkhome_path([""])

_APP_HOME = os.path.join(util.get_apps_dir(), _APP_NAME)
_app_local_directory = os.path.join(_APP_HOME, "local")
_BIN_PATH = os.path.join(_APP_HOME, "bin")

kl = KennyLoggins()
log = kl.get_logger(_APP_NAME, "modularinput", log.DEBUG)

log.debug("logging setup complete")

if isCloud():
    log.info("the sky is falling!! Clouds!")
else:
    log.info("no clouds. safe. much ground")

MI = GoogleAppsForSplunkModularInput(_APP_NAME, {
    "title": "G Suite For Splunk",
    "description": "The G Suite App will connect to your G Suite instance and pull Audit data for the domain.",
    "args": [
        {"name": "domain",
         "description": "The G Suite Domain to query for information",
         "title": "G Suite Domain",
         "required": True
         },
        {"name": "servicename",
         "description": "API To READ (report:all, see README for full list)",
         "title": "Report Key",
         "required": True
         },
        {"name": "extraconfig",
         "description": "Include extra configuration options for various API calls",
         "title": "Extra Configuration - JSON"
         },
        {"name": "proxy_name", "description": "The Proxy Stanza to use for data collection", "title": "proxy_name"}
    ]
})


def credentials_to_dict(credentials):
    return {'token': credentials.get("access_token"),
            'refresh_token': credentials.get("refresh_token"),
            'token_uri': credentials.get("token_uri"),
            'client_id': credentials.get("client_id"),
            'client_secret': credentials.get("client_secret"),
            'scopes': credentials.get("scopes")}


def run():
    MI.start()
    try:
        log.info("action=starting_modular_input_run")
        MI.set_logger(log)
        utils = Utilities(app_name=_APP_NAME, session_key=MI.get_config("session_key"))
        domain = MI.get_config("domain").lower()
        servicenames = [MI.get_config("servicename")]
        if "," in MI.get_config("servicename"):
            servicenames = MI.get_config("servicename").split(",")
        extConf = None
        running_backfill = False
        try:
            extConf = json.loads(MI.get_config("extraconfig"))
            log.debug("loaded_configuration {}".format(extConf))
            if "historical_days" in extConf:
                MI.checkpoint_default_lookback((extConf["historical_days"] * 1440))
            if "backfill" in extConf:
                # ASA-115, but only supported for Analytics.
                running_backfill = True
            log.debug("configuration {}".format(extConf))
            log.debug("question performing_backfill={}".format(running_backfill))

        except Exception as e:
            MI._catch_error(
                Exception("operation=load_extra_config error_message='%s' config='%s'" % (e, MI.get_config("extraconfig"))))
            sys.exit(_SYS_EXIT_FAILED_CONFIG)

        log.info("action=getting_credentials ref=DESK-194 domain={}".format(domain))
        goacd = utils.get_credential(_APP_NAME, domain)
        log.info("action=getting_credentials ref=DESK-194 domain={} goacd_type={}".format(domain, type(goacd)))
        google_oauth_credentials = None
        log.info("action=getting_credentials type={} is_str={}".format(type(goacd), isinstance(goacd, str)))
        if isinstance(goacd, str):
            try:
                google_oauth_credentials = json.loads(goacd.replace("'", '"'))
                log.info("action=getting_credentials loaded=true")
            except Exception as e:
                log.error("operation=load_credentials config={} msg={}".format(MI.get_config("name"), e))
                MI._catch_error(
                    Exception("operation=load_credentials config={} msg={}".format(MI.get_config("name"), e)))
        if goacd is None:
            MI._catch_error(
                Exception("operation=load_credentials error_message={} config={}".format("No Credentials Found in Store",
                                                                               MI.get_config("name"))))
            sys.exit(_SYS_EXIT_FAILED_GET_OAUTH_CREDENTIALS)
        log.info("action=getting_credentials type_is_dict={}".format(isinstance(google_oauth_credentials, dict)))
        assert type(google_oauth_credentials) is dict
        log.info("action=getting_credentials msg=setting_up_http")
        MI.setup_http_session(credentials_to_dict(google_oauth_credentials), _app_local_directory)
        MI.source("gapps:{}".format(MI.get_config("domain")))
        log.info("action=data_collection msg=starting_loop")
        for rr in servicenames:
            servicename_configuration = rr.split(":")
            report_name = servicename_configuration[0].strip()
            report_section = servicename_configuration[1].strip()
            log.info("action=data_collection rn={} rs={}".format(report_name, report_section))
            try:
                if report_name == "report":
                    def do_report(r):
                        MI.debug("status=run apikey={} apivalue={}".format(r, report_name))
                        _chkpoint_key = "{}_{}_{}".format(domain, report_name, r)
                        _chkpoint = MI.get_checkpoint(_chkpoint_key)
                        MI.sourcetype("gapps:{}:{}".format(report_name, r))
                        MI.info("type=service_call status=start ak={} av={}".format(r, report_name))
                        MI.gapps_admin_sdk_reports(applicationName=r, checkpoint=_chkpoint,
                                                   interval=MI.get_config("interval"))
                        MI.info("type=service_call status=stop ak={} av={}".format(r, report_name))
                        MI.set_checkpoint(_chkpoint_key)

                    if report_section == "all":
                        for ak in MI.available_apis[report_name]:
                            if "all" == ak:
                                MI.debug("not gonna get us.")
                            else:
                                do_report(ak)
                    else:
                        do_report(report_section)
                elif report_name == "analytics":
                    MI.debug("status=run apikey={} apivalue={}".format(report_section, report_name))
                    _chkpoint_key = "{}_{}_{}".format(domain, report_name, report_section)
                    if running_backfill:
                        _chkpoint_key = "{}_{}_{}_{}".format(domain, report_name, report_section, "backfill")
                    _chkpoint = MI._get_checkpoint(_chkpoint_key)
                    log.debug("checkpoint found_checkpoint {}".format(_chkpoint))
                    if _chkpoint is None:
                        log.debug("checkpoint checkpoint None")
                        _chkpoint = {"execution_time": 0, "completed_days": [], "is_backfill": running_backfill}
                        log.debug("checkpoint checkpoint set {}".format(_chkpoint))
                    if isinstance(_chkpoint, int) or isinstance(_chkpoint, float):
                        log.debug("checkpoint float int existing")
                        _chkpoint = {"execution_time": _chkpoint, "completed_days": [], "is_backfill": running_backfill}
                        log.debug("checkpoint converted_to_object {}".format(_chkpoint))
                    log.info("type=configured_checkpoint checkpoint={}".format(_chkpoint))
                    MI.sourcetype("google:{}:{}".format(report_name, report_section))
                    MI.info("type=service_call status=start ak={} av={}".format(report_section, report_name))
                    extConf["checkpoint"] = _chkpoint
                    if report_section == "report":
                        should_continue = True
                        max_counter = 0
                        while should_continue:
                            try:
                                MI.info("trying again: starting call for {}".format(MI.get_config("name")))
                                _chkpoint["completed_days"] = _chkpoint[
                                                                  "completed_days"] + MI.google_analytics_api_reports(
                                    **extConf)
                                MI.info("trying again: ending call for {}".format(MI.get_config("name")))
                                should_continue = False
                            except HTTPError as e:
                                MI.info(
                                    "trying again name='{}', counter='{}'".format(MI.get_config("name"), max_counter))
                                MI._catch_error(e)
                                max_counter += 1
                                if max_counter > 10:
                                    should_continue = False
                                time.sleep(10)
                    if report_section == "metadata":
                        MI.google_analytics_api_metadata(**extConf)
                    _chkpoint["execution_time"] = MI._loaded_checkpoints[_chkpoint_key]
                    MI._set_checkpoint(_chkpoint_key, _chkpoint)
                    MI.info("type=service_call status=stop ak={} av={}".format(report_section, report_name))
                elif report_name == "mail":
                    MI.debug("status=run apikey=%s apivalue=%s" % (report_section, report_name))
                    _chkpoint_key = "%s_%s_%s" % (domain, report_name, report_section)
                    _chkpoint = MI.get_checkpoint(_chkpoint_key)
                    MI.sourcetype("google:{}:{}".format(report_name, report_section))
                    MI.info("type=service_call status=start ak=%s av=%s" % (report_section, report_name))
                    MI.set_bin_directory(_BIN_PATH)
                    extConf["checkpoint"] = _chkpoint
                    MI.gapps_gmail(**extConf)
                elif report_name == "usage":
                    MI.sourcetype("gapps:{}:{}".format(report_name, report_section))
                    _chkpoint_key = "{}_{}_{}".format(domain, report_name, report_section)
                    _chkpoint = MI._get_checkpoint(_chkpoint_key)
                    if _chkpoint is None:
                        _chkpoint = {}
                    days_back = 7
                    if "historical_days" in extConf:
                        days_back = extConf["historical_days"]
                    usage = {}
                    last_date = None
                    if report_section in ["user", "customer"]:
                        start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
                        if "last_date" in _chkpoint:
                            start_date = _chkpoint["last_date"]
                        if report_section == "user":
                            usage = MI.usage_user_report(start_date)
                        elif report_section == "customer":
                            usage = MI.usage_customer_report(start_date)
                        else:
                            raise Exception("Report not found as configured: {}".format(report_section))
                        if usage is not None:
                            MI.info("action=usage_user user_usage_count={}".format(usage, len(usage)))
                            if len(usage) > 0:
                                MI.sourcetype("gapps:usage:{}:api".format(report_section))
                                MI.print_multiple_events(usage)
                                used_dates = [datetime.strptime(x["date"], "%Y-%m-%d") for x in usage if x["total_count"]>0]
                                used_dates.sort()
                                if len(used_dates)>0:
                                    MI.info("action=usage_user last={} sorted={}".format(used_dates[-1], used_dates))
                                    last_date = datetime.strftime(used_dates[-1], "%Y-%m-%d")
                            MI.info("{}".format(last_date))
                            if last_date is not None:
                                _chkpoint["last_date"] = last_date
                            MI._set_checkpoint(_chkpoint_key, object=_chkpoint)
                    elif report_section in ["chrome"]:
                        MI.get_usage_chrome_os_devices()
                elif report_name == "admin":
                    MI.sourcetype("gapps:{}:{}".format(report_name, report_section))
                    _chkpoint_key = "{}_{}_{}".format(domain, report_name, report_section)
                    _chkpoint = MI._get_checkpoint(_chkpoint_key)
                    if _chkpoint is None:
                        _chkpoint = {}
                    usage = {}
                    if report_section in ["users"]:
                        if report_section == "users":
                            usage = MI.admin_directory_users()
                        else:
                            raise Exception("Report not found as configured: {}".format(report_section))
                        MI._set_checkpoint(_chkpoint_key, object=_chkpoint)
                elif report_name == "alerts":
                    def do_alert(r):
                        MI.debug("status=run apikey={} apivalue={}".format(r, report_name))
                        _chkpoint_key = "{}_{}_{}".format(domain, report_name, r)
                        _chkpoint = MI.get_checkpoint(_chkpoint_key)
                        MI.sourcetype("gapps:{}:{}".format(report_name, r))
                        MI.info("type=service_call status=start ak={} av={}".format(r, report_name))
                        MI.get_alert_center_alerts(source=r, checkpoint=_chkpoint,
                                                   interval=MI.get_config("interval"))
                        MI.info("type=service_call status=stop ak={} av={}".format(r, report_name))
                        MI.set_checkpoint(_chkpoint_key)

                    if report_section == "all":
                        for ak in MI.available_apis[report_name]:
                            if "all" == ak:
                                MI.debug("not gonna get us.")
                            else:
                                do_alert(ak)
                    else:
                        do_alert(report_section)
                else:
                    raise Exception("Report not found as configured: {0}".format(json.dumps(servicename_configuration)))
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                log.error("{}".format({"timestamp": MI.gen_date_string(), "log_level": "ERROR", "msg": str(e),
                                       "exception_type": "{}".format(type(e)),
                                       "exception_arguments": "{}".format(e),
                                       "filename": fname,
                                       "exception_line": exc_tb.tb_lineno
                                       }))
                MI._catch_error(e)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("{}".format({"timestamp": MI.gen_date_string(), "log_level": "ERROR", "msg": str(e),
                             "exception_type": "{}".format(type(e)),
                             "exception_arguments": "{}".format(e),
                             "filename": fname,
                             "exception_line": exc_tb.tb_lineno
                             }))
        MI._catch_error(e)
    MI.info("action=stop item=modular_input")
    MI.stop()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == "--scheme":
            MI.scheme()
        elif sys.argv[1] == "--validate-arguments":
            MI.validate_arguments()
        elif sys.argv[1] == "--test":
            print('No tests for the scheme present')
        else:
            print('You giveth weird arguments')
    else:
        run()

    sys.exit(0)
