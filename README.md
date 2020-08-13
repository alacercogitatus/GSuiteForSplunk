# Welcome to G Suite for Splunk Apps documentation!

# Overview

## About G Suite For Splunk

|                           |                                                             |
| ------------------------- | ----------------------------------------------------------- |
| Author                    | Kyle Smith                                                  |
| App Version               | 1.4.3                                                       |
| App Build                 | 3                                                           |
| Vendor Products           | G Suite utilizing OAuth2                                    |
| Has index-time operations | true, the included TA add-on must be placed on the indexers |
| Creates an index          | false                                                       |
| Implements summarization  | Currently, the app does not generate summaries              |

About G Suite For Splunk

G Suite For Splunk allows a Splunk Enterprise administrator to interface with G Suite, consuming the usage and administrative logs provided by Google. The limitations on collection times are specified: [https://support.google.com/a/answer/7061566](https://support.google.com/a/answer/7061566) .

## Scripts and binaries

This App provides the following scripts:

|                  |                                                                                                        |
| ---------------- | ------------------------------------------------------------------------------------------------------ |
| ga.py            | This python file controls the ability to interface with the Google APIs.                               |
| ga_authorize.py | This Python custom endpoint allows the authorization of the App to G Suite For Splunk from the web UI. |
| Diag.py          | Allows diag-targeted collection of information.                                                        |
| ModularInput.py  | Inheritable Class to create Modular Inputs                                                             |
| Utilities.py     | Allows utility interactions with Splunk Endpoints                                                      |

Scripts

# Release notes

## Version 1.4.3

  - Improvement
    
      - [GSUITE-28] Enable TA changes for video conferencing
    
      - Removed Drive Data Enhancements due to volume and error issues

## Version 1.4.2

  - Improvement
    
      - [GSUITE-25] - Cloud App Vetting
    
      - [GSUITE-26] - Fix and Update Proxy settings

## Version 1.4.1

  -   - Test and QA
        
          - [GSUITE-23] - App inspect Failures

  -   - Bug
        
          - [GSUITE-19] - Interval check doesnt adjust for default.
        
          - [GSUITE-20] - BigQuery not caching last row

  -   - Improvement
        
          - [GSUITE-21] - Fix Proxy Code
        
          - [GSUITE-24] - Create New Dashboards

## Version 1.4.0

  - New Feature
    
      - [GSUITE-4][EXPERIMENTAL] - GMAIL LOGS and BigQuery
    
      - [GSUITE-15] - Splunk 8 Compatibility
    
      - [GSUITE-16] - Directory API Ingestion

  - Improvement
    
      - [GSUITE-12] - Auto Discover Available Spreadsheets
    
      - [GSUITE-14][Experimental] - Alert Center API
    
      - Added chat, gcp, meet, jamboard to allowed Reports input.

## About this release

Version 1.4.3 of G Suite For Splunk is compatible with:

|                            |                   |
| -------------------------- | ----------------- |
| Splunk Enterprise versions | 8.0               |
| Platforms                  | Splunk Enterprise |

Compatability

## Known Issues

Version 1.4.3 of G Suite For Splunk has the following known issues:

  - According to stackoverflow, there are indications that the Google Apps Admin API has an unspecified delay introduced into the events that are collected. This is most likely due to how Google collects the events and the global nature of the events. To mitigate this issue, the G Suite For Splunk Modular Input has a built-in delay in the consumption of events. If you run the modular input at 30 minutes, there will be a 30 minute delay of events. If you run at 1 hour, there will be a 1 hour delay in events.

  - References
    
      - [https://support.google.com/a/answer/7061566](https://support.google.com/a/answer/7061566)
    
      - [http://stackoverflow.com/questions/27389354/minimal-delay-when-listing-activities-using-the-reports-api](http://stackoverflow.com/questions/27389354/minimal-delay-when-listing-activities-using-the-reports-api)
    
      - [http://stackoverflow.com/questions/30850838/what-is-the-delay-between-a-event-happens-and-it-is-reflected-in-admin-reports-a](http://stackoverflow.com/questions/30850838/what-is-the-delay-between-a-event-happens-and-it-is-reflected-in-admin-reports-a)

  - These are the currently requested scopes:
    
      - [https://www.googleapis.com/auth/admin.reports.audit.readonly](https://www.googleapis.com/auth/admin.reports.audit.readonly)
    
      - [https://www.googleapis.com/auth/admin.reports.usage.readonly](https://www.googleapis.com/auth/admin.reports.usage.readonly)
    
      - [https://www.googleapis.com/auth/analytics.readonly](https://www.googleapis.com/auth/analytics.readonly)
    
      - [https://www.googleapis.com/auth/admin.directory.user.readonly](https://www.googleapis.com/auth/admin.directory.user.readonly)
    
      - [https://www.googleapis.com/auth/admin.directory.device.chromeos.readonly](https://www.googleapis.com/auth/admin.directory.device.chromeos.readonly)
    
      - [https://www.googleapis.com/auth/drive.metadata.readonly](https://www.googleapis.com/auth/drive.metadata.readonly)

# Support and resources

## Questions and answers

Access questions and answers specific to G Suite For Splunk at [https://answers.splunk.com](https://answers.splunk.com) . Be sure to tag your question with the App.

## Support

  - Support Email: [splunkapps@kyleasmith.info](mailto:splunkapps%40kyleasmith.info)

  - Support Offered: Community Engagement

Support is available via email at [splunkapps@kyleasmith.info](mailto:splunkapps%40kyleasmith.info). You can also find the author on IRC (\#splunk on efnet.org) or Slack. Feel free to email or ping, most responses will be within 1-2 business days.

# Installation and Configuration

## Software requirements

### Splunk Enterprise system requirements

Because this App runs on Splunk Enterprise, all of the [Splunk Enterprise system requirements]([https://docs.splunk.com/Documentation/Splunk/latest/Installation/Systemrequirements](https://docs.splunk.com/Documentation/Splunk/latest/Installation/Systemrequirements)) apply.

## Download

Download G Suite For Splunk at [https://splunkbase.splunk.com](https://splunkbase.splunk.com).

## Installation steps

NOTE: Where referenced, the IA-GSuiteForSplunk and TA-GSuiteForSplunk versions of this App are located on Splunkbase.

### Deploy to single server instance

1\. Deploy as you would any App, and restart Splunk. 1. NOTE: Only the App (or IA for no dashboards) is required. Install only 1 of the G Suite add ons or app. 1. Configure.

### Deploy to Splunk Cloud

1\. Have your Splunk Cloud Support handle this installation. Do NOT install the IA on the same system as the App. 1. You may consider using an on-premise Heavy Forwarder to install IA-GSuiteForSplunk, and send the logs to Splunk Cloud.

### Deploy to a Distributed Environment

1\. For each Search Head in the environment, deploy a non-configured copy of the App. DO NOT SEND TA or IA to a Search Head Cluster (SHC). 1. For each indexer in the environment, deploy a copy of the TA-GSuiteForSplunk Add-On that is located as mentioned above. 1. For a single Data Collection Node OR Heavy Forwarder (a full instance of Splunk is required), install IA-GSuiteForSplunk and configure through the GUI.

# User Guide

## Key concepts for G Suite For Splunk

  - You must have enabled the G Suite APIs at [https://console.developers.google.com](https://console.developers.google.com)

  - You must have configured a credential for use with this App at [https://console.developers.google.com](https://console.developers.google.com).

  - You must AUTHORIZE this app to make requests into G Suite APIs.

  - Scopes Defined are here: [https://developers.google.com/identity/protocols/googlescopes](https://developers.google.com/identity/protocols/googlescopes)

## Configure G Suite For Splunk for use with G Suite Admin Reporting.

Requires: `Admin SDK API`, and `Google Drive API` Optional: `Google Analytics Reporting API` Each API endpoint has individual APIs that need to be enabled within [https://console.developers.google.com](https://console.developers.google.com).

1.    - report:[all, gcp, chat, meet, jamboard, access_transparency, groups_enterprise, user_accounts, groups, mobile, admin, calendar, drive, login, token, rules]
        
        1.  These input service names require the *Admin SDK API* enabled.
        
        2.  Additionally, the drive report requires the *Google Drive API* enabled.
        
        3.  These inputs generally do not require Extra Configuration options in the Modular Input. An empty {} is still needed where advanced features are not.
        
        4.  These inputs should be adjusted per Google guidelines for the different activities.
        
        5.  By default, the Modular Input will only pull the previous 24 hours of data to prevent memory overflows.

2.    - analytics:[metadata, report]
        
        1.  These input service names require the *Analytics Reporting API v4* and *Analytics API* APIs enabled.
        
        2.  These inputs do require Extra Configuration. These inputs should not be enabled lightly, and require a little bit of prior research and planning.
        
        3.  IF YOU DONT KNOW WHAT THIS IS, DO NOT ENABLE IT
        
        4.  THIS IS A DARK FEATURE.

3.    - usage:[customer, user, chrome]
        
        1.  These input service names require the same as the report services.
        
        2.    - These inputs can have extra configuration, namely historical_days to do the initial data ingestion.
                
                1.  When configuring the modular input, use the Extra Configuration option of {historical_days: 180}
        
        3.  IMPORTANT: BE CAREFUL WITH USER REPORTING. If you ingest 365 days of data (back fill the information), you will end up with 365 \* \# of users events to pull and could cause a Splunk/System failure.
        
        4.  If you see a 404 Error in the logs relating to the usage reports, THESE ARE NORMAL.
        
        5.  The Customer Usage *should* include classrooms usage by default.

## Configure G Suite For Splunk for use with Google Spreadsheets

Requires: `Google Sheets API`

1.  When setting up the modular input, make sure you grab the Spreadsheet ID from the URL of the spreadsheet you need. Auto-discovery of available spreadsheets is not available (but an ER is in for it).

### Spreadsheet Destinations

1.    - Index
        
        1.  Takes the information from the sheet and indexes it to the specified index. This is useful to get lookups from a Heavy Forwarder to a Search head.
        
        2.  Use the provided Dashboard to re-assemble via saved scheduled search.

2.    - KVStore
        
        1.  Takes the information from the sheet and places it into a KVStore collection.
        
        2.  It will create the needed collections and transforms if needed.
        
        3.  Order of the COLUMNS `is NOT` kept, and the KVStore will be sorted via ASCII sort based on the column name.

3.    - Ordered KVStore
        
        1.  Takes the information from the sheet and places it into a KVStore collection.
        
        2.  It will create the needed collections and transforms if needed.
        
        3.  Order of the COLUMNS `IS` kept, the column names are stored in `ROW 0`

4.    - CSV Lookup
        
        1.  Takes the information from the sheet and places it into a CSV based lookup.
        
        2.  It will create the needed transforms if needed.
        
        3.  Order of the COLUMNS `is NOT` kept, and the CSV lookup will be sorted via ASCII sort based on the column name.

5.    - Ordered CSV Lookup
        
        1.  Takes the information from the sheet and places it into a CSV based lookup.
        
        2.  It will create the needed transforms if needed.
        
        3.  Order of the COLUMNS `IS` kept, the column names are stored in `ROW 0`.

## Configure G Suite For Splunk for use with Google BigQuery

Requires: `` `BigQuery API``

NOTE: This is EXPERIMENTAL. Enjoy breaking the input. This section to be updated when working correctly. NOTE: DOES NOT CURRENTLY WORK WITH PROXIES NOTE: To consume *all* tables in a dataset, use the table name *all*

### Requirements

1.  Service Account JSON File from GCP. ([https://console.developers.google.com/iam-admin/serviceaccounts](https://console.developers.google.com/iam-admin/serviceaccounts))

2.  Create a new Splunk credential with Realm: gsuite_bigquery and username is \<your_domain> (your domain as configured in the input)

3.  The password for that credential is the *ENTIRE* *ON ONE LINE* JSON file from GCP for the service account.

## Configure G Suite For Splunk for use with G Suite Admin Reporting

Requires: `G Suite Alert Center API` Note: `EXPERIMENTAL` (scope not valid) \#. alerts:[all, takeout, gmail, identity, operations, state, mobile]
 1.  These inputs generally do not require Extra Configuration options in the Modular Input. An empty {} is still needed where advanced features are not.  2.  By default, the Modular Input will only pull the previous 24 hours of data to prevent memory overflows.  3.  Uses the `https://www.googleapis.com/auth/apps.alerts` scope.  4.  View more information at [https://developers.google.com/admin-sdk/alertcenter/reference/alert-types](https://developers.google.com/admin-sdk/alertcenter/reference/alert-types) .

## Configure G Suite For Splunk for use with G Suite Classroom Reporting

Requires: `Google Classroom API`

1.    - courses:[aliases, announcements, work, submit, students, teachers, topics]
        
        1.  These input service names require the *Google Classroom API* enabled.
        
        2.  These inputs generally do not require Historical option in the Modular Input.
        
        3.  These inputs should be adjusted per Google guidelines for the different activities.
        
        4.  By default, the Modular Input will only pull the previous 24 hours of data to prevent memory overflows.

2.    - classroom:[invitations, registrations, profiles]
        
        1.  These input service names require the *Google Classroom API* enabled.

## Notes

IMPORTANT: You must Authorize the APIS with the SAME USER that allowed access to the APIs in the developer console (for GSuite customers - GCP see below).

Overview of authorization procedures are found here: [https://developers.google.com/identity/protocols/OAuth2ServiceAccount#overview](https://developers.google.com/identity/protocols/OAuth2ServiceAccount#overview).

GCP Users: It has been tested to use an Credential generated in the GCP console (same credential type as outlined on the OAuth App Config page). You can use an authorized admin to Approve the OAuth Scopes. It is unknown what happens when the approving Admin user account is disabled.

## Modular Input

**NOTE:** You will need to configure a new modular input for each domain

1.  Follow the steps on the Application Configuration dashboard to configure the modular input.

**NOTE:** After testing in a much bigger environment, weve been able to set these recommendations for intervals. You will need 4 modular input definitions.

1.  calendar, token, mobile, groups, login, saml, Chrome OS Devices \#. These are done at an cron interval of 15 \*/4 \* \* \*

2.  drive \#. Drive is done at a seconds interval of 600 - 1200 depending on organization size, and traffic flow of drive operations.

3.  Usage - User, Customer \#. These are done at a seconds interval of 86400

4.  admin, rules, chat, gplus \#. These are done at a seconds interval of 600

## Indexes

By default all events will be written to the main index. You should change the index in the configuration files to match your specific index.

## Configure Proxy Support

This App Supports proxy configuration. Configure the proxy first in the Application Configuration dashboard, and then choose it during the modular input configuration. The proxy name MUST BE gapps_proxy for the authorization to work correctly.

## Troubleshoot G Suite For Splunk

1.  Check the Monitoring Console (>=v6.5) for errors

2.  Visit the Application Health dashboard

3.  Search for eventtype=googleapps_error

## CIM

As of v1.4.0 of this app, we should support version 4.15 of the CIM.

## EXPERIMENTAL

There are portions of this app that are experimental, or you might see odd code. This is for some up coming features, might work, might not.

## Lookups

G Suite For Splunk contains the following lookup files:

1.  `gsuite_labels.csv` : This allows pretty labels on select dashboards.

## Event Generator

G Suite For Splunk does not make use of an event generator. This allows the product to display data, when there are no inputs configured.

## Acceleration

1.  Summary Indexing: No

2.  Data Model Acceleration: No

3.  Report Acceleration: No

## Binary File Declaration

1.  `bin/google/protobuf/internal/_api_implementation.so` is apparently a binary file. Required for Google Things.

2.  `bin/google/protobuf/internal/_message.so` is apparently a binary file. Required for Google Things.

For these two, please see [https://github.com/protocolbuffers/protobuf/tree/3.6.x/python/google/protobuf/internal](https://github.com/protocolbuffers/protobuf/tree/3.6.x/python/google/protobuf/internal) for source and attribution.

# Third Party Notices

Version 1.4.3 of G Suite For Splunk incorporates the following Third-party software or third-party services.

## Google Apps APIs

Please visit [https://developers.google.com/google-apps/](https://developers.google.com/google-apps/) for full terms and conditions.

## Aplura, LLC Components

Components Written by Aplura, LLC Copyright (C) 2016-2017 Aplura, ,LLC

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 2 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, eMA 02110-1301, USA.

## defusedxml

## defusedxml

PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2

1\. This LICENSE AGREEMENT is between the Python Software Foundation (PSF), and the Individual or Organization (Licensee) accessing and otherwise using this software (Python) in source or binary form and its associated documentation.

2\. Subject to the terms and conditions of this License Agreement, PSF hereby grants Licensee a nonexclusive, royalty-free, world-wide license to reproduce, analyze, test, perform and/or display publicly, prepare derivative works, distribute, and otherwise use Python alone or in any derivative version, provided, however, that PSFs License Agreement and PSFs notice of copyright, i.e., Copyright (c) 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008 Python Software Foundation; All Rights Reserved are retained in Python alone or in any derivative version prepared by Licensee.

3\. In the event Licensee prepares a derivative work that is based on or incorporates Python or any part thereof, and wants to make the derivative work available to others as provided herein, then Licensee hereby agrees to include in any such work a brief summary of the changes made to Python.

4\. PSF is making Python available to Licensee on an AS IS basis. PSF MAKES NO REPRESENTATIONS OR WARRANTIES, EXPRESS OR IMPLIED. BY WAY OF EXAMPLE, BUT NOT LIMITATION, PSF MAKES NO AND DISCLAIMS ANY REPRESENTATION OR WARRANTY OF MERCHANTABILITY OR FITNESS FOR ANY PARTICULAR PURPOSE OR THAT THE USE OF PYTHON WILL NOT INFRINGE ANY THIRD PARTY RIGHTS.

5\. PSF SHALL NOT BE LIABLE TO LICENSEE OR ANY OTHER USERS OF PYTHON FOR ANY INCIDENTAL, SPECIAL, OR CONSEQUENTIAL DAMAGES OR LOSS AS A RESULT OF MODIFYING, DISTRIBUTING, OR OTHERWISE USING PYTHON, OR ANY DERIVATIVE THEREOF, EVEN IF ADVISED OF THE POSSIBILITY THEREOF.

6\. This License Agreement will automatically terminate upon a material breach of its terms and conditions.

7\. Nothing in this License Agreement shall be deemed to create any relationship of agency, partnership, or joint venture between PSF and Licensee. This License Agreement does not grant permission to use PSF trademarks or trade name in a trademark sense to endorse or promote products or services of Licensee, or any third party.

8\. By copying, installing or otherwise using Python, Licensee agrees to be bound by the terms and conditions of this License Agreement.

## markdown.js

Released under the MIT license.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the Software), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED AS IS, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## jquery.js

[https://github.com/jquery/jquery/blob/master/LICENSE.txt](https://github.com/jquery/jquery/blob/master/LICENSE.txt)

Copyright JS Foundation and other contributors, [https://js.foundation/](https://js.foundation/)

This software consists of voluntary contributions made by many individuals. For exact contribution history, see the revision history available at [https://github.com/jquery/jquery](https://github.com/jquery/jquery)

The following license applies to all parts of this software except as documented below:

-----

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the Software), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED AS IS, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

-----

All files located in the node_modules and external directories are externally maintained libraries used by this software which have their own licenses; we recommend you read them, as their terms may differ from the terms above.

## d3.js

[https://github.com/d3/d3/blob/master/LICENSE](https://github.com/d3/d3/blob/master/LICENSE)

Copyright 2010-2016 Mike Bostock All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

  - Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

  - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

  - Neither the name of the author nor the names of contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS AS IS AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# [G Suite For Splunk](#)

### Navigation

### Related Topics

  - [Documentation overview](#)

2017, alacercogitatus. | Powered by [Sphinx 3.1.1](http://sphinx-doc.org/) & [Alabaster 0.7.12](https://github.com/bitprophet/alabaster)
