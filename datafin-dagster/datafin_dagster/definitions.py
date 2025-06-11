from dagster import Definitions, EnvVar


#resources
from .resources.credentials import SecretsResource


#defintions
from .polygon.definitions import polygon_definitions


# global_resources = {
#     "secrets": SecretsResource(
#         aws_access_key=EnvVar("PERSONAL_AWS_ACCESS_KEY"),
#         aws_secret_access_key=EnvVar("PERSONAL_AWS_SECRET_ACCESS_KEY"),
#         region_name="us-east-1"
#     )
# }

defs = Definitions.merge(
    polygon_definitions,
    Definitions(resources={
         "secrets": SecretsResource(
        aws_access_key=EnvVar("PERSONAL_AWS_ACCESS_KEY"),
        aws_secret_access_key=EnvVar("PERSONAL_AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1"
    )})
)