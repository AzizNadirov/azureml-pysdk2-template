"""static settings for pipeline."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """c lass - a set of settings for pipelines."""

    BASE_DIR = Path(__file__).parent.parent
    components_dir: str = BASE_DIR / "components"

    ENV_DEFAULT_IMAGE = "mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest"
    ENV_DEFAULT_DESCRIPTION = "Environment created by DSML SDK v2: {env_name}"
    ENV_DEFAULT_VERSION = str(datetime.now().timestamp()).split(".")[0]  # noqa: DTZ005
    ENV_DEFAULT_TAGS = None
    DEFAULT_INFOFILE_NAME = "aml_pipeline_info.yaml"


settings = Settings()
