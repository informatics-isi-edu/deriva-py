import json
import logging
import requests
from datetime import datetime
from deriva.core import get_credential, get_new_requests_session, urlsplit, urlunsplit, format_exception, strtobool
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

    def process(self):
        url = self.parameters.get("url", self.WORKSPACE_API_URL)
        session = get_new_requests_session()
        headers = {"Content-Type": "application/json"}
        token = self.get_access_token_from_wallet("fair_research_data_portal", self.wallet)
        if token:
            headers.update({"Authorization": "Bearer %s" % token})
        tasks = self.parameters.get("tasks", ["WES", "JUPYTERHUB"])
        for k, v in self.outputs.items():
            minid = v[IDENTIFIER_KEY]
            metadata = self.parameters.get("metadata",
                                           {"grouping": "DERIVA Export",
                                            "data_set": "%s" % k,
                                            "data_id": "%s" % datetime.now().isoformat()})
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
