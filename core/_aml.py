import os
from numbers import Number
from pathlib import Path
from typing import Any, Literal, Optional, Union

from azure.ai.ml import Input, MLClient, Output
from loguru import logger

from core.ds_constants import get_ml_client

logger.level = "INFO"


def get_aml_uri(short_path: str) -> str:
    """Takes azure ml datastore "short-path" and returns azureml fs uri.
    Params:
        short_path: str - path with format `datastore_name: path/to/somewhare`.
    """
    uri = "azureml://subscriptions/{subs}/resourcegroups/{rg}/workspaces/{ws}/datastores/{ds}/paths/{pth}"
    ds_name = short_path.split(":")[0].strip()
    path_on_ds = short_path.split(":")[1].strip()
    if path_on_ds.startswith("/"):
        path_on_ds = path_on_ds[1:]  # rm leading '/'
    uri = uri.format(
        subs=os.environ["SUBSCRIPTION_ID"],
        rg=os.environ["RESOURCE_GROUP"],
        ws=os.environ["WORKSPACE_NAME"],
        ds=ds_name,
        pth=path_on_ds,
    )
    return uri


class DataSchema:
    """Schema for AML Input/Output data entities."""

    # as default will take first one from the list
    IO_MODES = {
        "INPUT": ["ro_mount", "download", "direct", "rw_mount"],    # rw_mount working, but docs says it is not
        "OUTPUT": ["rw_mount", "upload", "direct"],
    }

    INPUTS = (
        "uri_folder",
        "uri_file",
        "mltable",
        "mlflow_model",
        "custom_model",
        "integer",
        "number",
        "string",
        "boolean",
    )

    def __init__(
        self: "DataSchema",
        data_type: Union[
            Literal[
                "uri_folder",
                "uri_file",
                "mltable",
                "mlflow_model",
                "custom_model",
                "integer",
                "number",
                "string",
                "boolean",
            ],
            None,
        ] = None,
        default_value: Optional[str] = None,
        description: Optional[str] = None,
        ml_client: Optional[MLClient] = None,
    ) -> None:
        """Data Schema abstraction for Azure ML Input/Output data entities.

        Handles supported literals, Custom URI, or standard Azure URI formats.
        Use `as_input()` or `as_output()` to convert to appropriate class instance.

        Args:
            data_type (Union[Literal, None]): Type of the data entity. Must be one of:
                'uri_folder', 'uri_file', 'mltable', 'mlflow_model', 'custom_model',
                'integer', 'number', 'string', 'boolean'. Defaults to None.
            default_value (str, optional): URI path to data/asset or Custom URI to datastore
                in format `<datastore_name>:path/to/data`. Can be file or folder.
                Defaults to None.
            description (str, optional): Description of the data. Defaults to None.
            ml_client (MLClient, optional): Azure ML client instance. Defaults to None.
        """

        self.default_value = default_value
        self.data_type = data_type
        self.description = description
        self.client = ml_client if ml_client else get_ml_client()

        self.__validate()

    def __validate(self: "DataSchema"):
        logger.info("Validating Data")
        assert (
            self.data_type in self.INPUTS
        ), f"Unsupported data type: {self.data_type}. \n\tMust be one of: {self.INPUTS}"

    def __get_ds_uri(self, short_uri: str) -> str:
        """Build uri according to the custom uri(curi)
        self.default_value - CURI like `<datastore_name>:/path/to/data`.
        """
        logger.info(f"Building uri for {short_uri}")
        # extract creds from client
        subscription_id = self.client.subscription_id
        resource_group = self.client.resource_group_name
        workspace = self.client.workspace_name
        # get datastore name:
        #   I guess, there are will be some one who will write as `<datastorename>:/path/to/data`, so check and fix it
        ind = short_uri.index(":")
        # ':' is last - dont do anything
        if len(short_uri) != ind + 1 and short_uri[ind + 1] == "/":
            # remove symbol at index i+1 --> '/'
            short_uri = short_uri[: ind + 1] + short_uri[ind + 2 :]
        logger.debug(f"path turned to {short_uri}")

        ds_name = short_uri.split(":")[0]
        data_path = short_uri.split(":")[1]
        uri = f"""azureml://subscriptions/{subscription_id}/resourcegroups/{resource_group}/workspaces/{workspace}/datastores/{ds_name}/paths/{data_path}"""  # NOQA E501

        logger.info(f"Built uri: {uri}")
        return uri

    def __to_aml(
        self: "DataSchema",
        as_: Literal["input", "output"],
        value: Union[str, Number, bool, None] = None,
        mode: Union[str, None] = None,
        **kwargs: Any,
    ) -> Union[Input, Output]:
        """Convert datatype and input mode to necessary class instance."""
        # has value
        assert as_ in ["input", "output"]
        if value is None and self.default_value is None:
            msg = "Data has no value"
            raise ValueError(msg)

        value = value or self.default_value
        value = self.__value2uri(value)
        # if value not a uri, take back
        if not self.data_type:
            self.data_type = self.__guess_dtype(value)
        # look for mode
        if not mode:
            mode = self.IO_MODES["INPUT"][0] if as_ == "input" else self.IO_MODES["OUTPUT"][0]

        if as_ == "input":
            return Input(
                path=value,
                type=self.data_type,
                mode=mode,
                description=self.description,
                **kwargs,
            )
        else:
            return Output(
                path=value,
                type=self.data_type,
                mode=mode,
                description=self.description,
                **kwargs,
            )

    def __value2uri(self, value: Union[str, Number, bool]) -> Union[str, None]:
        """check, if value needs to be converted to aml."""

        def has_uri_prefix(value: str) -> bool:
            uri_prefixes = [
                "azureml:",
                "https://",
                "http://",
                "wasbs://",
                "abfss://",
                "adl://",
            ]
            return any(value.startswith(prefix) for prefix in uri_prefixes)

        def is_local_path(path: str) -> bool:
            # todo: not a clear way RM[3]
            try:
                path = Path(path)
                return path.is_file() or path.is_dir()
            except Exception:
                return False

        # if value is correct aml uri

        if isinstance(value, str) and has_uri_prefix(value):
            return value
        if isinstance(value, str) and ":" in value:
            return self.__get_ds_uri(value)
        if is_local_path(value):
            return value
        else:
            return None

    def __guess_dtype(self: "DataSchema", value: Union[str, Number, bool]) -> Union[str, None]:
        # ? RM-sources[2]
        # Check for boolean first, before int
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, str) and value.startswith("azureml:"):
            # file or dir ?
            if "." in value:
                return "uri_file"
            else:
                return "uri_folder"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, float):
            return "number"
        elif isinstance(value, int):
            return "integer"
        return None
        # todo: look for other variants- mlflow...

    def as_input(
        self: "DataSchema",
        value: Union[str, Number, bool, None] = None,
        mode: Literal["ro_mount", "download", "direct", "rw_mount"] = "ro_mount",
        **kwargs: Any,
    ) -> Input:
        """Build Input instance from Data schema.
        Args:
            value (Union[str, Number, bool, None], optional): fill schema with this value. Defaults to None.
            mode (Literal["ro_mount", "download", "direct", "rw_mount], optional): input mode. Defaults to "ro_mount".
            **kwargs: _description_
        Raises:
            ValueError: in case of unsupported mode
        Returns:
            Input: ` azure.ai.ml.Input` instance with passed or default-schema value.
        """
        if self.default_value is None and value is None:
            msg = "Data has no a default value nor a passed value"
            raise ValueError(msg)
        if mode not in self.IO_MODES["INPUT"]:
            msg = f"Unsupported mode: {mode}. \n\tMust be one of: {self.IO_MODES['INPUT']}"
            raise ValueError(msg)
        # passed value can overwrite default one
        return self.__to_aml(as_="input", mode=mode, value=value, **kwargs)

    def as_output(
        self: "DataSchema",
        value: Union[str, Number, bool, None] = None,
        mode: Literal["rw_mount", "upload", "direct"] = "rw_mount",
        **kwargs: Any,
    ) -> Output:
        """Build Output instance from Data schema.
        Args:
            value (Union[str, Number, bool, None], optional): fill schema with this value. Defaults to None.
            mode (Literal["rw_mount", "upload", "direct"], optional): input mode. Defaults to "rw_mount".
            **kwargs: _description_
        Raises:
            ValueError: in case of unsupported mode
        Returns:
            Output: ` azure.ai.ml.Output` instance with passed or default-schema value.
        """
        if self.default_value is None and value is None:
            msg = "Data has no a default value nor a passed value"
            raise ValueError(msg)
        if mode not in self.IO_MODES["OUTPUT"]:
            msg = f"Unsupported mode: {mode}. \n\tMust be one of: {self.IO_MODES['OUTPUT']}"
            raise ValueError(msg)
        # passed value can overwrite default one
        return self.__to_aml(as_="output", mode=mode, value=value, **kwargs)

    def __str__(self: "DataSchema") -> str:
        return f"""Data:
                    \tDefault value: {self.default_value}
                    \tdata_type: {self.data_type}
                    """

    def __repr__(self: "DataSchema") -> str:
        return self.__str__()
