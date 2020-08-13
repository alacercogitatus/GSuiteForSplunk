import sys
from splunk.appserver.mrsparkle.lib.util import make_splunkhome_path
_APP_NAME = 'GSuiteForSplunk'
import os.path
import multiprocessing.dummy as mp
from itertools import product

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

_MI_APP_NAME = 'G Suite For Splunk Modular Input - Classroom'
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
log = kl.get_logger(_APP_NAME, "classroom_modularinput", log.DEBUG)

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
        {"name": "class_servicename",
         "description": "API To READ (courses:all, see README for full list)",
         "title": "Report Key",
         "required": True
         },
        {"name": "historical",
         "description": "Set the historical lookback",
         "title": "Historical Days"
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
        log.info("action=starting_classroom_modular_input_run")
        MI.set_logger(log)
        utils = Utilities(app_name=_APP_NAME, session_key=MI.get_config("session_key"))
        domain = MI.get_config("domain").lower()
        servicenames = [MI.get_config("class_servicename")]
        if "," in MI.get_config("class_servicename"):
            servicenames = MI.get_config("class_servicename").split(",")
        if MI.get_config("historical"):
            MI.checkpoint_default_lookback((MI.get_config("historical") * 1440))
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
                Exception("operation=load_credentials realm={} domain={} error_message={} config={}".format(_APP_NAME, domain,
                                                                                                            "No Credentials Found in Store",
                                                                               MI.get_config("name"))))
            sys.exit(_SYS_EXIT_FAILED_GET_OAUTH_CREDENTIALS)
        log.info("action=getting_credentials type_is_dict={}".format(isinstance(google_oauth_credentials, dict)))
        assert type(google_oauth_credentials) is dict
        log.info("action=getting_credentials msg=setting_up_http")
        MI.setup_http_session(credentials_to_dict(google_oauth_credentials), _app_local_directory)
        MI.source("gapps:{}".format(MI.get_config("domain")))
        log.info("action=data_collection msg=starting_loop")
        p = mp.Pool(4)

        def classroom_report(service, cr_courses):
            log.info("action=do_classroom_report report={} course={}".format(service, cr_courses))
            if len(cr_courses) < 1:
                return
            MI.threaded_classroom_report(service, course=cr_courses)
        courses = MI.courses(write_courses=("courses:write" in servicenames))
        matrix = [(x, y) for x in servicenames for y in courses]
        log.info("action=call_thread matrix={}".format(matrix))
        p.starmap(classroom_report, matrix)
        p.close()
        p.join()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error(
            "{} log_level={} message={} exception_type={} exception_arguments={} filename={} exception_line={}".format(
                MI.gen_date_string(), "ERROR", str(e), "{}".format(type(e)), "{}".format(e), fname, exc_tb.tb_lineno))
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
