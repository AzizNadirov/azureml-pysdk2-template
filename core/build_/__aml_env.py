import re
from datetime import datetime
from pathlib import Path
from typing import List, Union

import yaml
from azure.ai.ml.entities import Environment
from azure.core.exceptions import ResourceNotFoundError
from loguru import logger
from pydantic import BaseModel, ValidationError

from core.ds_constants import get_ml_client
from core.settings import settings


# Env Schemas
class PipDependencies(BaseModel):
    pip: list[str]


class EnvironmentSchema(BaseModel):
    name: str
    channels: Union[list[str], None] = None
    dependencies: list[Union[str, PipDependencies]]

    def compare(self: "EnvironmentSchema", other: "EnvironmentSchema") -> dict:
        diff = {}
        if self.name != other.name:
            diff["name"] = (self.name, other.name)

        if self.channels != other.channels:
            diff["channels"] = self._compare_channels(other)

        deps_diff = self._compare_dependencies(other)
        if deps_diff:
            diff["dependencies"] = deps_diff

        return diff

    def is_equal(self: "EnvironmentSchema", other: "EnvironmentSchema") -> bool:
        return len(self.compare(other)) == 0

    def _compare_channels(self: "EnvironmentSchema", other: "EnvironmentSchema") -> Union[tuple, dict]:
        if self.channels is None and other.channels is None:
            return {}
        if self.channels is None:
            return ("missing", other.channels)
        if other.channels is None:
            return (self.channels, "missing")
        return set(self.channels).symmetric_difference(set(other.channels))

    def _compare_dependencies(self: "EnvironmentSchema", other: "EnvironmentSchema") -> dict:
        diff = {}
        if len(self.dependencies) != len(other.dependencies):
            diff["length"] = (len(self.dependencies), len(other.dependencies))

        for i, (self_dep, other_dep) in enumerate(
            zip(self.dependencies, other.dependencies),
        ):
            if isinstance(self_dep, str) and isinstance(other_dep, str):
                if self_dep != other_dep:
                    diff[f"item_{i}"] = (self_dep, other_dep)
            elif isinstance(self_dep, PipDependencies) and isinstance(
                other_dep,
                PipDependencies,
            ):
                if self_dep.pip != other_dep.pip:
                    diff[f"pip_{i}"] = set(self_dep.pip).symmetric_difference(
                        set(other_dep.pip),
                    )
            else:
                diff[f"type_mismatch_{i}"] = (type(self_dep), type(other_dep))

        return diff


def increment_version(version_string: str, increment: bool = True) -> str:
    """Increments passed version string, if uses timestamp if version not numeric."""
    pattern = r"(\d+)\.(\d+)\.(\d+)"
    default_version = str(datetime.now().timestamp()).split(".")[0]
    if not (re.match(pattern, version_string) or version_string.isdigit()):
        logger.warning(
            f"Env version label {version_string} does not match pattern {pattern}, so return '{default_version}' ",
        )
        return default_version

    if version_string.isdigit():
        return increment and f"{int(version_string) + 1}" or f"{int(version_string) - 1}"

    parts = list(map(int, version_string.split(".")))
    if increment:
        parts[-1] += 1
    else:
        parts[-1] -= 1
    return ".".join(map(str, parts))


class Env:
    """class contains aml env based ops."""

    ml_client = get_ml_client()

    def __init__(
        self: "Env",
        env_name: str,
        conda_file_path: Union[str, Path] = "./conda_dependencies.yaml",
    ) -> None:
        """Parameters
        env_name: str - name of the env
        conda_file_path: str - path to conda file.
        """
        self.env_name = env_name
        self.conda_file_path = Path(conda_file_path)
        self.__validate()

    def __validate(self):
        if not self.conda_file_path.exists():  # todo: mb more informative
            msg = f"Conda file not found: {self.conda_file_path}"
            raise ValueError(msg)

    @classmethod
    def get_latest(cls: "Env", env_name: str) -> Environment:
        """Get latest version of aml env with given name.
        Actually, you can use `ml_client.environments.get(name, label='latest')`,
        but it will return 'Anonymous' environment, without any version.
        """
        try:
            env_list = list(cls.ml_client.environments.list(name=env_name))
            return [env for env in env_list if env.properties["azureml.labels"] == "latest"][0]
        except ResourceNotFoundError:
            return None

    @classmethod
    def get_if_exists(cls: "Env", name: str, version: Union[str, None] = None) -> Union[Environment, None]:
        """Get aml env with given name."""
        if version:
            try:
                return cls.ml_client.environments.get(
                    name,
                    version=version,
                    label="latest" if version is None else None,
                )
            except ResourceNotFoundError:
                return None

        # get latest version
        return cls.get_latest(name)

    def get_create_or_update(
        self: "Env",
        create_version: str = None,
        create_image: str = None,
        create_description: str = None,
        create_tags: List[str] = None,
        interactive: bool = False,
    ) -> Environment:
        """Get, Update or Create aml env.
        If env exists, check if it has changed according to the conda file,
        if changed - update it, else return it.
        If env doesn't exist, create it and return new created env.

        Parameters
        ----------
            create_version: str - version if env will be created
            create_image: str - image if env will be created
            create_description: str - description if env will be created
            create_tags: List[str] - tags if env will be created
            interactive: bool - ask user for some parameters in while.
        """
        # 1. check if it exists:
        latest = self.get_latest(self.env_name)
        # if doesn't exist, create it
        if latest is None:
            new_env = Environment(
                name=self.env_name,
                description=create_description or settings.ENV_DEFAULT_DESCRIPTION.format(env_name=self.env_name),
                tags=create_tags or settings.ENV_DEFAULT_TAGS,
                conda_file=self.conda_file_path.as_posix(),
                image=create_image or settings.ENV_DEFAULT_IMAGE,
            )

            self.ml_client.environments.create_or_update(new_env)
            return new_env

        # Env is exists, check if it has changed according to the conda file:
        logger.info(
            f"Environment {self.env_name} found with latest version: {latest.version}",
        )
        logger.info(f"Looking for changes in conda file: {self.conda_file_path}")

        with open(self.conda_file_path) as f:
            local_conda = yaml.safe_load(f).copy()
        remote_conda = latest.conda_file.copy()

        # fill schemas
        try:
            local_schema = EnvironmentSchema(**local_conda)
            remote_schema = EnvironmentSchema(**remote_conda)
        except ValidationError as e:
            logger.error(e)
            msg = "Unable to create EnvironmentSchema from conda file. Looks like it's not a valid conda file."
            raise ValueError(
                msg,
            )

        # if env changed - update it!
        if not remote_schema.is_equal(local_schema):
            diff = local_schema.compare(remote_schema)
            logger.info(
                f"Local context is different from remote one:\n{diff}\nUpdating...",
            )
            # define new version
            if interactive is True:
                new_version = input(
                    f"Enter new version for {self.env_name} (current version: {latest.version}).\
                    Default will be '{increment_version(latest.version)}': ",
                )
                if not new_version.strip():
                    new_version = increment_version(latest.version)
            else:
                new_version = increment_version(latest.version)

            updated_env = Environment(
                name=self.env_name,
                version=new_version,
                description=latest.description,
                tags=latest.tags,
                properties=latest.properties,
                conda_file=local_conda,
                build=latest.build,
                image=latest.image,
            )
            logger.info(
                f"Pushing {updated_env.name} with version {updated_env.version}...",
            )
            self.ml_client.environments.create_or_update(updated_env)
            logger.info(
                f"Environment '{updated_env.name}' successfully updated from\
                {latest.version} to {updated_env.version}.",
            )
            return updated_env

        # env is not changed
        logger.info(f"Environment {self.env_name} is up to date.")
        return latest


# https://dsmlstoragexfsyt.blob.core.windows.net/25d827f3-cf10-4e2a-b65d-40c316812ddd-kjo0hlz7of1dtv5im8iuvb9en5/preprocess/main.py
