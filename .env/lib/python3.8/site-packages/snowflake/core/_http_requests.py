"""Helpers related to sending/receiving HTTP requests."""

import ssl
import typing

import urllib3


class SFPoolManager(urllib3.PoolManager):
    # Having this typed is non-trivial across multiple
    #  urllib3 major versions
    def request(  # type: ignore[no-untyped-def,override]
        self,
        session_token,
        method,
        url: str,
        fields=None,
        headers: typing.Optional[typing.Dict[str, str]]=None,
        **urlopen_kw: typing.Any,
    ):
        if url_needs_auth(url):
            if headers is None:
                headers = dict()
            headers.update(get_session_headers(session_token))
        return super().request(
            method=method,
            url=url,
            fields=fields,
            headers=headers,
            **urlopen_kw
        )

# Use a connection pool singleton for every resource
CONNECTION_POOL: typing.Optional[SFPoolManager] = None

def get_session_headers(session_token: str) -> typing.Dict[str, str]:
    return {
        "Authorization": f"Snowflake Token=\"{session_token}\"",
    }

def url_needs_auth(url: str) -> bool:
    """Whether a URL needs the authentication headers to work.

    For now this is all URLs, since Python connector takes care of authentication and
    session related actions.
    """
    return True

# TODO: We could create the single connection pool at import time
#  instead of having this function at all
# TODO: Configuration classes have no single parent class
def create_connection_pool(  # type: ignore[no-untyped-def]
        configuration,
        pools_size: int=4,
        maxsize:typing.Optional[int]=None,
    ) -> SFPoolManager:
    # TODO: locking?
    global CONNECTION_POOL
    if CONNECTION_POOL is None:
        # urllib3.PoolManager will pass all kw parameters to connectionpool
        # https://github.com/shazow/urllib3/blob/f9409436f83aeb79fbaf090181cd81b784f1b8ce/urllib3/poolmanager.py#L75  # noqa: E501
        # https://github.com/shazow/urllib3/blob/f9409436f83aeb79fbaf090181cd81b784f1b8ce/urllib3/connectionpool.py#L680  # noqa: E501
        # maxsize is the number of requests to host that are allowed in parallel
        # Custom SSL certificates and client certificates: http://urllib3.readthedocs.io/en/latest/advanced-usage.html  # noqa: E501

        # cert_reqs
        if configuration.verify_ssl:
            cert_reqs = ssl.CERT_REQUIRED
        else:
            cert_reqs = ssl.CERT_NONE

        addition_pool_args = {}
        if configuration.assert_hostname is not None:
            addition_pool_args['assert_hostname'] = configuration.assert_hostname

        if configuration.retries is not None:
            addition_pool_args['retries'] = configuration.retries

        if configuration.socket_options is not None:
            addition_pool_args['socket_options'] = configuration.socket_options

        if maxsize is None:
            if configuration.connection_pool_maxsize is not None:
                maxsize = configuration.connection_pool_maxsize
            else:
                maxsize = 4

        # https pool manager
        cp_kwargs ={
            "num_pools": pools_size,
            "maxsize": maxsize,
            "cert_reqs": cert_reqs,
            "ca_certs": configuration.ssl_ca_cert,
            "cert_file": configuration.cert_file,
            "key_file": configuration.key_file,
            **addition_pool_args
        }
        if configuration.proxy:
            cp_kwargs.update(
                {
                    "proxy_url": configuration.proxy,
                    "proxy_headers": configuration.proxy_headers,
                }
            )
        CONNECTION_POOL = SFPoolManager(**cp_kwargs)
    return CONNECTION_POOL
