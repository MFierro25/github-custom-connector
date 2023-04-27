#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

# Basic full refresh stream
class GithubClones(HttpStream):

    # Base Url
    url_base = f"https://api.github.com/repos/"
    primary_key = None
    
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.owner = config['owner']
        self.repo = config['repo']
        self.bearer_token = config['bearer_token']
        self.github_owner = self.owner
        self.github_repo = self.repo
        

    def path(
        self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        # The "/latest" path gives us the latest currency exchange rates
        self.__class__.url_base = f"https://api.github.com/repos/{self.github_owner}/{self.github_repo}/traffic/clones"
        return self.__class__.url_base  

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        # The api requires that we include apikey as a header so we do that in this method
        headers = super().request_headers(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        headers.update({"Accept": "application/vnd.github+json",
                "Authorization": "Bearer " + self.bearer_token,})
        return headers

    # def request_params(
    #         self,
    #         stream_state: Mapping[str, Any],
    #         stream_slice: Mapping[str, Any] = None,
    #         next_page_token: Mapping[str, Any] = None,
    # ) -> MutableMapping[str, Any]:
    #     return {'owner': self.owner, 'repo': self.repo}

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        # The response is a simple JSON whose schema matches our stream's schema exactly, 
        # so we just return a list containing the response
        return [response.json()]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # The API does not offer pagination, 
        # so we return None to indicate there are no more pages in the response
        return None
    
    def get_json_schema(self):
        schema = super().get_json_schema()
        schema['dynamically_determined_property'] = "property"
        return schema

# Source
class SourcePersonalGithubExample(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        logger.info("Checking github API connection...")
        
        headers = {
                "Accept": "application/vnd.github+json",
                "Authorization": "Bearer " + config["bearer_token"],
            }
        url = (f"https://api.github.com/repos/{config['owner']}/{config['repo']}/traffic/clones")
        
        try:
        
            resp = requests.get(url, headers=headers)
            status = resp.status_code
            logger.info(f"Ping response code: {status}")
            if status == 200:
                return True, None

            error = resp.json()
            message = error.get("errorDescription") or error.get("error")
            return False, message
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token="bearer_token") 
        return [GithubClones(authenticator=auth, config=config)]
