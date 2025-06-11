from dagster import ConfigurableResource

from datafin.aws import SecretsClient                      #type: ignore

class SecretsResource(ConfigurableResource):
    aws_access_key: str
    aws_secret_access_key: str
    region_name: str = "us-east-1"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._secrets_client = None
    
    @property
    def client(self) -> SecretsClient:
        if self._secrets_client is None:
            self._secrets_client = SecretsClient(
                aws_access_key=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
        return self._secrets_client