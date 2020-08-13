from __future__ import absolute_import
import sys
import os.path

from splunk.appserver.mrsparkle.lib.util import make_splunkhome_path
_APP_NAME = 'GSuiteForSplunk'

sys.path.insert(0, make_splunkhome_path(["etc", "apps", _APP_NAME, "bin", "lib"]))
sys.path.insert(0, make_splunkhome_path(["etc", "apps", _APP_NAME, "bin", "lib", "python3.7", "site-packages"]))

import logging as log
import json
import splunk.appserver.mrsparkle.lib.util as util
from splunk.appserver.mrsparkle.lib.util import isCloud
from GoogleAppsForSplunkModularInput import GoogleAppsForSplunkModularInput
from Utilities import KennyLoggins, Utilities

__author__ = 'ksmith'
_MI_APP_NAME = 'G Suite For Splunk Modular Input - Big Query'
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
log = kl.get_logger(_APP_NAME, "ga_bigquery_modularinput", log.INFO)

log.debug("logging setup complete")

if isCloud():
   log.info("the sky is falling!! Clouds!")
else:
   log.info("no clouds. safe. much ground")

MI = GoogleAppsForSplunkModularInput(_APP_NAME, {
    "title": "G Suite For Splunk - Big Query",
    "description": "This Modular Input is designed specifically for Big Query requests to support Gmail Logs.",
    "args": [
        {"name": "domain",
         "description": "The G Suite Domain to query for information",
         "title": "G Suite Domain",
         "required": False
         },
        {"name": "project",
         "description": "Google Project ID",
         "title": "project",
        "required": True
         },
        {"name": "dataset",
         "description": "Google Dataset ID",
         "title": "dataset",
        "required": True
         },
        {"name": "table",
         "description": "<![CDATA[The Google BigQuery Data Table in the form <dataset>.<table>]]>",
         "title": "table",
         "required": True},
        {"name": "proxy_name", "description": "The Proxy Stanza to use for data collection", "title": "proxy_name", "required": False}
    ]
})


def run():
    MI.start()
    try:
        utils = Utilities(app_name=_APP_NAME, session_key=MI.get_config("session_key"))
        domain = MI.get_config("domain").lower()
        bq_project = MI.get_config("project")
        bq_table = MI.get_config("table")
        bq_dataset = MI.get_config("dataset")
        log.info("action=getting_credentials ref=DESK-194 domain={}".format(domain))
        goacd = utils.get_credential("gsuite_bigquery", domain)
        log.info("action=getting_credentials ref=DESK-194 domain={} app={} goacd_type={}".format(domain, "gsuite_bigquery",  type(goacd)))
        google_oauth_credentials = None
        if isinstance(goacd, str):

            import urllib.parse
            google_oauth_credentials = json.loads(urllib.parse.unquote(goacd).replace("'",'"'))
        if goacd is None:
            try:
                raise Exception("operation=load_credentials error_message={} config={}".format("No Credentials Found in Store",
                                                                               MI.get_config("name")))
            except Exception as e:
                MI._catch_error(e)
            sys.exit(_SYS_EXIT_FAILED_GET_OAUTH_CREDENTIALS)
        assert type(google_oauth_credentials) is dict
        log.info("Credential Type: {}".format(type(google_oauth_credentials)))
        # NEEDS A KEY PUT INTO CREDENTIAL STORE
        # https://console.developers.google.com/iam-admin/serviceaccounts
        ret_val = MI.setup_bigquery_session(google_oauth_credentials, _app_local_directory, bq_project)
        log.info("ret_val {}".format(ret_val))
        MI.source("gapps:{}".format(MI.get_config("domain")))
        try:
            _chkpoint_key = "{}_{}_{}_{}".format(domain, "bigquery", bq_dataset, bq_table)
            _chkpoint = MI._get_checkpoint(_chkpoint_key)
            log.debug("checkpoint found_checkpoint {}".format(_chkpoint))
            if _chkpoint is None:
                log.debug("checkpoint checkpoint None")
                _chkpoint = {"execution_time": 0, "completed_days": []}
                log.debug("checkpoint checkpoint set {}".format(_chkpoint))
            log.info("type=configured_checkpoint checkpoint={}".format(_chkpoint))
            MI.sourcetype("google:{}:{}".format("bigquery", bq_table))
            log.info("starting_to_grab={}".format(bq_table))
            if bq_table == "all":
                MI.bigquery_ingest_all_tables(bq_project, bq_dataset)
            else:
                MI.bigquery_query_all_fields_by_table(bq_project, bq_dataset, bq_table)
            _chkpoint["execution_time"] = MI._loaded_checkpoints[_chkpoint_key]
            MI._set_checkpoint(_chkpoint_key, _chkpoint)
            MI.info("type=service_call status=stop ak={} av={}".format("bigquery", bq_table))
        except Exception as e:
            MI._catch_error(e)
            myJson = {"timestamp": MI.gen_date_string(), "log_level": "ERROR"}
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            myJson["errors"] = [{"msg": str((e)),
                                 "exception_type": "%s" % type(e),
                                 "exception_arguments": "%s" % e,
                                 "filename": fname,
                                 "exception_line": exc_tb.tb_lineno,
                                 "input_name": MI.get_config("name")
                                 }]
            oldst = MI.sourcetype()
            MI.sourcetype("{}:error".format(_APP_NAME))
            MI.print_error("{}".format(json.dumps(myJson)))
            log.error("{}".format((json.dumps(myJson))))
            MI.sourcetype(oldst)
    except Exception as e:
        MI._catch_error(e)
        myJson = {"timestamp": MI.gen_date_string(), "log_level": "ERROR"}
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        myJson["errors"] = [{"msg": str((e)),
                             "exception_type": "%s" % type(e),
                             "exception_arguments": "%s" % e,
                             "filename": fname,
                             "exception_line": exc_tb.tb_lineno,
                             "input_name": MI.get_config("name")
                             }]
        oldst = MI.sourcetype()
        MI.sourcetype("{}:error".format(_APP_NAME))
        MI.print_error("{}".format(json.dumps(myJson)))
        log.error("{}".format((json.dumps(myJson))))
        MI.sourcetype(oldst)
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
