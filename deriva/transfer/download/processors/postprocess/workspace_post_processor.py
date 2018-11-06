import json
import logging
import requests
from datetime import datetime
from deriva.core import get_credential, get_new_requests_session, urlsplit, urlunsplit, format_exception, stob
from deriva.core.utils.webauthn_utils import get_wallet_entries
from deriva.transfer.download import DerivaDownloadError, DerivaDownloadConfigurationError
from deriva.transfer.download.processors.base_processor import *


class GlobusWorkspacePortalPostProcessor(BaseProcessor):
    """
    Post processor that POSTs minids and metadata to the Globus Workspace Portal at:
    https://globus-portal.fair-research.org

    This processor is DEMO-ware and will be removed from the core transfer package after a few months.
    """

    WORKSPACE_API_URL = "https://globus-portal.fair-research.org/4M.4.Fullstacks/api/v1/workspaces/"

    def __init__(self, envars=None, **kwargs):
        super(GlobusWorkspacePortalPostProcessor, self).__init__(envars, **kwargs)

    def _get_access_token_from_wallet(self):
        entries = get_wallet_entries(
            self.wallet, "oauth2",
            credential_source="https://auth.globus.org",
            resource_server="fair_research_data_portal",
            scopes=["https://auth.globus.org/scopes/ebcaf30d-8148-4f1b-992a-bd089f823ac7/workspace_manager"])
        token = entries[0].get("access_token") if entries else None
        return token

    def process(self):
        url = self.parameters.get("url", self.WORKSPACE_API_URL)
        session = get_new_requests_session()
        headers = {"Content-Type": "application/json"}
        token = self._get_access_token_from_wallet()
        if token:
            headers.update({"Authorization": "Bearer %s" % token})
        tasks = self.parameters.get("tasks", ["WES", "JUPYTERHUB"])
        data_set = self.parameters.get("data_set")
        data_id = self.parameters.get("data_id")
        for k, v in self.outputs.items():
            minid = v[IDENTIFIER_KEY]
            logging.info("Registering minid [%s] with Workspace Portal: [%s]" % (minid, url))
            hostname = self.envars.get("hostname")
            hostname = "" if not hostname else " (%s)" % hostname
            metadata = self.parameters.get("metadata",
                                           {"grouping": "DERIVA%s" % hostname,
                                            "data_set": "%s" % data_set if data_set else k,
                                            "data_id": "%s" % data_id if data_id else datetime.now().isoformat()})
            workspaces = v.get("workspaces", [])
            body = {
                "input_minid": minid,
                "tasks": tasks,
                "metadata": metadata
            }
            try:
                response = session.post(url, json=body, headers=headers)
                response.raise_for_status()
                workspaces.append(response.json())
                v["workspaces"] = workspaces
            except Exception as e:
                raise DerivaDownloadError("Unable to create workspace: %s" % format_exception(e))

        return self.outputs
