from __future__ import absolute_import
import sys
import os.path
import splunk.appserver.mrsparkle.lib.util as util
from splunk.appserver.mrsparkle.lib.util import make_splunkhome_path
_APP_NAME = 'GSuiteForSplunk'

sys.path.insert(0, make_splunkhome_path(["etc", "apps", _APP_NAME, "bin", "lib"]))
sys.path.insert(0, make_splunkhome_path(["etc", "apps", _APP_NAME, "bin", "lib", "python3.7", "site-packages"]))

import logging as log
import json
import re
from splunk.appserver.mrsparkle.lib.util import isCloud
from GoogleAppsForSplunkModularInput import GoogleAppsForSplunkModularInput
from Utilities import KennyLoggins, Utilities

__author__ = 'ksmith'
_MI_APP_NAME = 'G Suite For Splunk Modular Input'
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
log = kl.get_logger(_APP_NAME, "ga_ss_modularinput", log.INFO)

log.debug("logging setup complete")

if isCloud():
    log.info("the sky is falling!! Clouds!")
else:
    log.info("no clouds. safe. much ground")

MI = GoogleAppsForSplunkModularInput(_APP_NAME, {
    "title": "G Sheets",
    "description": "The G Suite Sheet Sync.",
    "args": [
        {"name": "domain",
         "description": "The G Suite Domain.",
         "title": "G Suite Domain",
         "required": False
         },
        {"name": "ss_id",
         "description": "Sheet to Sync",
         "title": "Report Key",
         "required": True
         },
        {
            "name": "ss_sheet",
            "description": "Sheet Tab to Sync",
            "title": "Sheet Tab",
            "required": True
        },
        {"name": "destination",
         "description": "Where should we put this? Defaults to index. Values: index, kvstore, transform",
         "title": "Destination", "required": True},
        {"name": "proxy_name", "description": "The Proxy Stanza to use for data collection", "title": "proxy_name",
         "required": False}
    ]
})


def do_lookup_sheet(sheet, sheet_title, order, utils, destination):
    s_id = sheet[0]["metadata"]["sheet_properties"].get("title")
    t_name = re.sub('[^0-9a-zA-Z]+', '_', "gapps_csv_ss_{}_s_{}_transform".format(sheet_title, s_id).lower())
    filename = "{}.csv".format(t_name)
    ret_obj = {"transform_name": t_name, "filename": filename, "error": False, "metadata": sheet[0]["metadata"],
               "destination": destination, "order": order}
    log.info("action=checking_transform_exists filename={} sheet={} title={} order={}".format(filename, len(sheet), sheet_title, order))
    r = utils.check_transform_exists(t_name, do_create=True, filename=filename)
    fields = "fields"
    starting_index = 0
    if order == "ordered":
        fields = "ordered_fields"
        starting_index = -1
    if r is not None:
        utils.write_lookup(filename, [row.get(fields) for row in sheet if row.get("row_index") > starting_index])
    else:
        ret_obj["error"] = True
        ret_obj["error_message"] = "Failed to create Transforms Stanza, please see utilities log for detailed error."
    return ret_obj


def do_kvstore_sheet(sheet, sheet_title, order, utils, destination):
    s_id = sheet[0]["metadata"]["sheet_properties"].get("title")
    t_name = re.sub('[^0-9a-zA-Z]+', '_', "gapps_kvstore_ss_{}_s_{}_transform".format(sheet_title, s_id).lower())
    ret_obj = {"collection_name": t_name, "error": False, "metadata": sheet[0]["metadata"], "destination": destination,
               "order": order}
    fields = "fields"
    starting_index = 0
    if order == "ordered":
        fields = "ordered_fields"
        starting_index = -1
    fl = ["_key"]
    [fl.extend(x.get(fields).keys()) for x in sheet]
    fields_list = list(set(fl))
    log.info("action=kvstore_generate_fields_list fields_list={}".format(fields_list))
    r = utils.check_collection_exists(t_name, do_create=True,
                                      fields_list=fields_list)
    if r is not None:
        def set_list(row):
            list_fields = row.get(fields)
            list_fields["_key"] = "row_{}".format("{}".format(row.get("row_index")).rjust(10, '0'))
            return list_fields

        data = [set_list(row) for row in sheet if row.get("row_index") > starting_index]
        c_name = "{}_col".format(t_name)
        log.info("action=sync_to_kvstore action=deleting_existing_items collection={}".format(c_name))
        utils.delete_kvstore_all_items(c_name)
        log.info("action=sync_to_kvstore action=batch_save_items collection={}".format(c_name))
        ret_obj["kvstore_batch_save"] = utils.kvstore_batch_save(c_name, data)
    else:
        ret_obj["error"] = True
        ret_obj[
            "error_message"] = "Failed to create Collection or Transforms Stanzas, please see utilities log for detailed error."
    return ret_obj

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
        log.info("action=configuring_utils")
        utils = Utilities(app_name=_APP_NAME, session_key=MI.get_config("session_key"))
        domain = MI.get_config("domain").lower()
        dest = MI.get_config("destination")
        destination = dest
        order = "index"
        if ":" in dest:
            t = dest.split(":")
            destination = t[0]
            order = t[1]
        log.info("action=getting_credentials ref=DESK-194 domain={}".format(domain))
        goacd = utils.get_credential(_APP_NAME, domain)
        log.info("action=getting_credentials ref=DESK-194 domain={} goacd_type={}".format(domain, type(goacd)))
        google_oauth_credentials = None
        if isinstance(goacd, str):
            log.info("action=load_credentials")
            google_oauth_credentials = json.loads(goacd.replace("'", '"'))
        if goacd is None:
            MI._catch_error(
                "operation=load_credentials error_message={} config={}".format("No Credentials Found in Store",
                                                                               MI.get_config("name")))
            sys.exit(_SYS_EXIT_FAILED_GET_OAUTH_CREDENTIALS)
        log.info("action=asserting_dict type={}".format(type(google_oauth_credentials)))
        assert type(google_oauth_credentials) is dict
        log.info("action=setting_http_session")
        MI.setup_http_session(credentials_to_dict(google_oauth_credentials), _app_local_directory)
        MI.source("gapps:{}".format(MI.get_config("domain")))
        log.info("action=getting_sheet_configurations")
        log.info("action=getting_spreadsheet sheet_id={}".format(MI.get_config("ss_id")))
        sheet_information = MI.get_spreadsheets(MI.get_config("ss_id"), includeGridData=True)
        log.info("action=getting_sheets_ids ss_sheet={}".format(MI.get_config("ss_sheet")))
        individual_sheets = MI.get_config("ss_sheet").split(",")
        MI.sourcetype("gapps:spreadsheet")
        sheet_title = sheet_information.get("properties", {}).get("title", "").replace(" ", "_").lower()
        log.debug("action=checking_for_inclusion sheets_to_keep={} available_sheets={}".format(json.dumps(individual_sheets),
                                                                           json.dumps([{"id":
                                                                               sheet.get("properties", {}).get(
                                                                                   "sheetId"),
                                                                               "title": sheet.get("properties", {}).get(
                                                                                   "title")} for sheet in
                                                                               sheet_information.get("sheets", [])])))
        sheets = [sheet for sheet in sheet_information.get("sheets", []) if
                  "{}".format(sheet.get("properties", {}).get("sheetId")) in individual_sheets]
        log.debug("action=got_sheets sheet_count={}".format(len(sheets)))
        sheet_data = [MI.parse_spreadsheet_data({"title": sheet_title, "id": MI.get_config("ss_id")}, sheet)
                      for sheet in sheets]
        log.debug("action=parsed_sheets sheets={}".format(json.dumps(sheet_data)))
        if destination == "transform":
            MI.print_multiple_events([do_lookup_sheet(ss, sheet_title, order, utils, destination) for ss in sheet_data])
        elif destination == "kvstore":
            MI.print_multiple_events(
                [do_kvstore_sheet(ss, sheet_title, order, utils, destination) for ss in sheet_data])
        elif destination == "index":
            [MI.print_multiple_events(ss) for ss in sheet_data if ss is not None]
        else:
            log.fatal("action=no_destination destination={}".format(destination))
    except Exception as e:
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
