from __future__ import absolute_import
import sys
from splunk.appserver.mrsparkle.lib.util import make_splunkhome_path
import splunk.appserver.mrsparkle.lib.util as util

_APP_NAME = 'GSuiteForSplunk'
import os.path

sys.path.insert(0, make_splunkhome_path(["etc", "apps", _APP_NAME, "bin", "lib"]))
sys.path.insert(0, make_splunkhome_path(["etc", "apps", _APP_NAME, "bin", "lib", "python3.7"]))
sys.path.insert(0, make_splunkhome_path(["etc", "apps", _APP_NAME, "bin", "lib", "python3.7", "site-packages"]))
sys.path.insert(0, make_splunkhome_path(["etc", "apps", _APP_NAME, "bin", "lib", "python3.7", "site-packages", "apiclient"]))

import json
import logging
import os
import splunk

# Google Stuff
import httplib2
import socks
from Utilities import Utilities, KennyLoggins
from apiclient.discovery import build
import google.oauth2.credentials
dir = os.path.join(util.get_apps_dir(), _APP_NAME, 'bin', 'lib')

if not dir in sys.path:
    sys.path.append(dir)
httplib2.CA_CERTS = "{}/{}".format(os.path.join(util.get_apps_dir(), _APP_NAME, 'bin'), "cacerts.txt")
_LOCALDIR = os.path.join(util.get_apps_dir(), _APP_NAME, 'local')
if not os.path.exists(_LOCALDIR):
    os.makedirs(_LOCALDIR)

_SYS_EXIT_FAILED_GET_OAUTH_CREDENTIALS = 1

kl = KennyLoggins()
logger = kl.get_logger(_APP_NAME, "ga_spreadsheets_endpoint", logging.DEBUG)

def credentials_to_dict(credentials):
    return {'token': credentials.get("access_token"),
            'refresh_token': credentials.get("refresh_token"),
            'token_uri': credentials.get("token_uri"),
            'client_id': credentials.get("client_id"),
            'client_secret': credentials.get("client_secret"),
            'scopes': credentials.get("scopes")}

def get_session(utils, gapps_domain):
    try:
        logger.debug("operation=ca_certs location={}".format(httplib2.CA_CERTS))
        goacd = utils.get_credential(_APP_NAME, gapps_domain)
        logger.info(
            "action=getting_credentials ref=DESK-194 domain={} goacd_type={}".format(gapps_domain, type(goacd)))
        google_oauth_credentials = None
        if isinstance(goacd, str):
            google_oauth_credentials = json.loads(goacd.replace("'", '"'))
        if goacd is None:
            logger.error(
                "operation=load_credentials error_message=\"{}\"".format("No Credentials Found in Store"))
            sys.exit(_SYS_EXIT_FAILED_GET_OAUTH_CREDENTIALS)
        assert type(google_oauth_credentials) is dict
        proxy_config_file = os.path.join(_LOCALDIR, "proxy.conf")
        proxy_info = None
        if os.path.isfile(proxy_config_file):
            try:
                pc = utils.get_proxy_configuration("gapps_proxy")
                scheme = "http"
                if pc["useSSL"] == "true":
                    scheme = "https"
                logger.debug("action=setting_proxy scheme={} host={}, port={} username={}".format(scheme,
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
                logger.warn("action=load_proxy status=failed message=No_Proxy_Information stanza=gapps_proxy")
        h = httplib2.Http(proxy_info=proxy_info)
        credentials = google.oauth2.credentials.Credentials(**credentials_to_dict(google_oauth_credentials))
        return h, credentials
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        myJson ={}
        myJson["errors"] = [{"msg": str(e),
                             "exception_type": "%s" % type(e),
                             "exception_arguments": "{}".format(e).replace('"', ''),
                             "filename": fname,
                             "exception_line": exc_tb.tb_lineno
                             }]
        logger.error(json.dumps(myJson))


class ga_ss(splunk.rest.BaseRestHandler):

    def __init__(self, method, requestInfo, responseInfo, sessionKey):
        splunk.rest.BaseRestHandler.__init__(self, method, requestInfo, responseInfo, sessionKey)
        self.utils = Utilities(app_name=_APP_NAME, session_key=sessionKey)
        self.operations = {"get_spreadsheets": self.get_spreadsheets}

    def get_spreadsheets(self, httpo=None, credentials=None, spreadsheetId=None, **kwargs):
        try:
            ss = build('sheets', 'v4', http=httpo, credentials=credentials)
            return ss.spreadsheets().get(spreadsheetId=spreadsheetId).execute()
        except Exception as e:
            raise Exception("{}".format(self._catch_error(e)))

    def get_spreadsheet_all(self, httpo=None, credentials=None, **kwargs):
        try:
            ss = build('drive', 'v3', http=httpo, credentials=credentials)
            mime_type = "application/vnd.google-apps.spreadsheet"
            query = "mimeType='{}'".format(mime_type)
            return ss.files().list(q=query).execute()
        except Exception as e:
            raise Exception("{}".format(self._catch_error(e)))

    def _catch_error(self, e):
        myJson = {"log_level": "ERROR"}
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        myJson["errors"] = [{"msg": str(e),
                             "exception_type": "%s" % type(e),
                             "exception_arguments": "{}".format(e).replace('"', ''),
                             "filename": fname,
                             "exception_line": exc_tb.tb_lineno
                             }]
        return myJson

    # /custom/GoogleAppsForSplunk/ga_authorize/build
    # @expose_page(must_login=False, methods=['GET'])
    def handle_GET(self, **kwargs):
        try:
            query_parameters = self.request["query"]
            gapps_domain = query_parameters.get("domain", "").strip().lower()
            logger.debug("operation=GET action=set field=domain value={}".format(gapps_domain))
            operation = query_parameters.get("op")
            if operation is None:
                raise Exception("Operation parameter op is None")
            logger.debug("operation=GET action=set field=operation value={}".format(operation))
            http_session, credentials = get_session(self.utils, gapps_domain)
            status = {}
            if operation == "get_spreadsheets":
                status.update(self.get_spreadsheets(http=http_session, credentials=credentials, spreadsheetId=query_parameters.get("spreadsheetId")))
            elif operation == "get_spreadsheets_all":
                status.update(
                    self.get_spreadsheet_all(http=http_session, credentials=credentials))
            return [{"operation": operation, "status": "success", "data": status}]
        except Exception as e:
            logger.error("{}".format(self._catch_error(e)))
            return [{"msg": "{}".format(e).replace('"', ''), "operation": "error"}]

    handle_POST = handle_GET
