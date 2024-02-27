"""Manages Snowpark Container Services.

Example:
    >>> new_service_def  = Service(
    ...     name="MYSERVICE",
    ...     compute_pool="MYCOMPUTEPOOL",
    ...     spec="@~/myservice_spec.yml",
    ...     min_instances=1,
    ...     max_instances=1,
    ... )
    >>> services = root.databases["MYDB"].schemas["MYSCHEMA"].services
    >>> myservice = services.create(new_service_def)
    >>> myservice_snapshot = myservice.fetch()
    >>> service_data = services.iter(like="%SERVICE")
    >>> myservice.suspend()
    >>> myservice.resume()
    >>> service_status = myservice.get_service_status()
    >>> logs = myservice.get_service_logs()
    >>> myservice.delete()
    >>> an_existing_service = services["an_existing_service"]
    >>> an_existing_service.suspend()

Refer to :class:`snowflake.core.Root` to create the ``root``.
"""


from public import public

from ..service._generated.models import ServiceSpecificationModel as ServiceSpecification
from ..service._generated.models import ServiceSpecInlineTextModel as ServiceSpecInlineText
from ..service._generated.models import ServiceSpecStageFileModel as ServiceSpecStageFile
from ._service import Service, ServiceCollection, ServiceResource


public(
    Service=Service,
    ServiceCollection=ServiceCollection,
    ServiceResource=ServiceResource,
    ServiceSpecification=ServiceSpecification,
    ServiceSpecInlineText=ServiceSpecInlineText,
    ServiceSpecStageFile=ServiceSpecStageFile,
)
