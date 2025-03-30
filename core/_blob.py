""" #!!beta - module for handling azure blob storage """
import os
import shutil
from pathlib import Path
from pprint import pprint
from typing import Union

import pandas as pd
import yaml
from azure.storage.blob import BlobClient, ContainerClient
from dotenv import load_dotenv
from core.utils import read_table


class Blob:
    """blob instance class"""

    def __init__(self, _blob: BlobClient):
        """
        Parameters:
            _blob: BlobClient - blob client
            name: str - name of the blob
            properties: BlobProperties - properties of the blob
        """
        self._blob = _blob
        self.name = _blob.blob_name
        self.properties = _blob.get_blob_properties()

    def download_blob(
        self, save_at: Union[str, Path] = "", return_df: bool = False, pd_read_kwargs: dict = None
    ) -> Union[None, pd.DataFrame]:
        """
        download blob into 'save_at' path.
        Parameters:
            save_at: Union[str, Path] - path to save the blob
            return_df: bool - read and return file as pandas dataframe
        """
        print(f"\tDownloading blob '{self.name}' size: {self.properties.size} bytes")
        data = self._blob.download_blob().readall()
        os.makedirs(save_at.parent, exist_ok=True)

        with open(save_at, "wb") as f:
            f.write(data)
        print(f"\t\tsaved as '{save_at}'")

        if return_df:
            return read_table(Path(save_at), kwargs=pd_read_kwargs)


class Container:
    """blob container instance class"""

    def __init__(self, _container: ContainerClient):
        self._container = _container
        self.name = _container.container_name

    def download_all(self, save_at: Union[str, Path] = "", keep_hierarchy: bool = True):
        """download all blobs in the container
        Parameters:
            save_at: Union[str, Path] - path to save the blobs
            keep_hierarchy: bool - keep folders structure
        """
        print("\nTry to download all blobs. Blobs:\n")
        pprint([b.name for b in self._container.list_blobs()])
        print()
        for blob in self._container.list_blobs():
            blob = self._container.get_blob_client(blob)
            save_at_ = Path(save_at) / blob.blob_name if keep_hierarchy else Path(save_at)
            blob = Blob(blob).download_blob(save_at=save_at_)

    def download_folder(self, folder: str, save_at: str = "", keep_hierarchy: bool = True):
        """
        download all blobs from the folder.
        Parameters:
            folder: str - name of the folder
            save_at: str - path to save the blobs
        """
        folder = Path(folder)
        save_at = Path(save_at)
        print(f"Downloading folder '{folder}' from container to '{save_at}'")
        # look for blobs in the folder
        all_blobs = self._container.list_blobs()
        blobs = [b for b in all_blobs if b.name.startswith(str(folder))]
        # convert to blob instances
        blobs = [Blob(self._container.get_blob_client(b)) for b in blobs if Path(b.name).is_file()]
        print("\tFound blobs: \n")
        pprint([b.name for b in blobs])
        print()
        for b in blobs:
            # print(f"\tDownloading '{b.name}' size: {b.properties.size} bytes")
            if keep_hierarchy:
                save_at_ = save_at / (Path(b.properties.name).relative_to(folder))
            else:
                filename = Path(b.name).name
                save_at_ = save_at / filename
            # print(f"save at: {save_at_}")
            b.download_blob(save_at=save_at_)

    def download_these(self, blob_names: list, save_at: str = "", pass_if_doesnt_exist: bool = False):
        """
        download blobs enumerated in 'blob_names'
        Parameters:
            blob_names: list - list of blob names
            save_at: str - path to save the blobs
            pass_if_doesnt_exist: bool - skip if the blob does not exist
        """
        _diff = set(blob_names) - set([b.name for b in self._container.list_blobs()])

        if pass_if_doesnt_exist and _diff:
            print(f"WARNING: these blobs not found[{len(blob_names)}/{len(_diff)}]: {list(_diff)}")

        elif not pass_if_doesnt_exist and _diff:
            raise FileNotFoundError(f"these blobs not found[{len(blob_names)}/{len(_diff)}]: {list(_diff)}")

        # download
        for blob_name in set(blob_names) - _diff:
            blob = self._container.get_blob_client(blob_name)
            filename = Path(blob_name).name
            if Path(save_at).is_file():
                save_at = Path(save_at)
            else:
                save_at = Path(save_at) / filename
            blob = Blob(blob).download_blob(save_at=save_at)

    def upload(self, data, to: str, overwrite: bool = False):
        """
        Upload data to the container
        Parameters:
            data: path to the data or byte-like data
            to: name of the blob, destination
            overwrite: bool - overwrite if exists
        """
        print(f"Uploading data to '{to}'")
        # check data
        if isinstance(data, (str, Path)):
            print("try to read file")
            data = open(data, "rb")

        blob = self._container.get_blob_client(to)
        blob.upload_blob(data, overwrite=overwrite)
        print("\tDone")


class BlobHandler:
    """class for doing Blob related things"""

    def __init__(self, sas_url_container: str = None, sas_url_blob: str = None):
        """
        Class for doing Blob related things.
        Parameters:
            sas_url_container: sas url for container
            sas_url_blob: sas url for blob
        Attributes:
            container: Container - container instance
            blob: Blob - blob instance
        """
        self.__validate(sas_url_container, sas_url_blob)
        if sas_url_container:
            self.container = Container(ContainerClient.from_container_url(sas_url_container))
        if sas_url_blob:
            self.blob = BlobClient.from_blob_url(sas_url_blob)

    def __validate(self, sas_url_container, sas_url_blob):
        if not sas_url_blob and not sas_url_container:
            raise ValueError("sas_url_container or sas_url_blob must be provided")
        elif sas_url_container and sas_url_blob:
            print(
                "WARNING! Both sas_url_container and sas_url_blob provided. \
                  Use 'blob' and 'container' attributes for gett access to blob or container"
            )


class LocalFileUpdater:
    """class for updating local files according to the 'local_file_updater.yaml' file"""

    yaml_file_name = "local_file_updater.yaml"

    def __init__(self, directory: Path, dotenv_path: Path = "./.env"):
        """ """
        self.directory = directory
        self.dotenv_path = dotenv_path

    @classmethod
    def __get_sass_interactively(cls, yaml_dict: dict) -> None:
        """parse container names and take user input for sas urls"""
        # list of unique containers
        containers = list(set([list(blob_row.values())[0].split(":")[0].upper() for blob_row in yaml_dict["files"]]))
        vars_ = {}
        for container_name in containers:
            sas_url = input(f"Enter sas url for '{container_name}': ")
            vars_[f"BLOB-{container_name}"] = sas_url

        # export new env vars
        os.environ.update(vars_)

    def load_dotenv(self):
        if self.dotenv_path.exists():
            load_dotenv(self.dotenv_path)
        else:
            raise FileNotFoundError(f".env file not found: '{self.dotenv_path}'")

    def validate_yaml(self):
        """ """
        yaml_path = self.directory / self.yaml_file_name
        if not yaml_path.exists():
            raise FileNotFoundError(f"File not found: '{yaml_path}'")

        # valideate content

    @staticmethod
    def __get_sas_url(blob_container_name: str):
        sas_url = os.environ.get(f"BLOB-{blob_container_name.upper()}")
        if not sas_url:
            raise ValueError(
                f"Environment variable not found: BLOB-{blob_container_name.upper()} for {blob_container_name}"
            )
        return sas_url

    @classmethod
    def __update_file(cls, local_file: Path, blob_file: str):
        """ """
        blob_container_name, blob_file = blob_file.split(":")
        sas_url = cls.__get_sas_url(blob_container_name)

        # download blob to local
        BlobHandler(sas_url_container=sas_url).container.download_these(
            [
                blob_file,
            ],
            save_at=str(local_file),
        )

    @classmethod
    def __handle_yaml_row(cls, file: str, updater_path: Path, only_files_in_dir: bool, keep_onlylocals: bool) -> None:
        """ """
        local_file, blob_file = tuple(file.items())[0]
        local_file = Path(updater_path / local_file).resolve()
        if local_file.is_dir():
            cls.__handle_yaml_row_dir(local_file, blob_file, only_files_in_dir, keep_onlylocals)
        else:
            cls.__handle_yaml_row_file(local_file, blob_file)

    @classmethod
    def __handle_yaml_row_file(cls, local_file: Path, blob_file: Path):
        cls.__update_file(local_file, blob_file)

    @classmethod
    def __handle_yaml_row_dir(
        cls, local_dir: Path, blob_dir: str, only_files_in_dir: bool, keep_onlylocals: bool
    ) -> None:
        """replace files inside 'local_dir' with files from 'blob_dir'"""
        # print('blob_dir: ', blob_dir)
        container_name, blob_dir = blob_dir.split(":")
        blob_dir = Path(blob_dir)
        # print('blob_dir: ', blob_dir)
        if only_files_in_dir is True:
            for local_file in local_dir.rglob("*.*"):
                blob_file = blob_dir / local_file.relative_to(local_dir)
                cls.__update_file(local_file, blob_file)
        else:
            print(f"Transfer blob dir: '{blob_dir}' into local dir: '{local_dir}'")
            if keep_onlylocals is True:
                # take each blob and download into local, this will overwrite file if already exists

                BlobHandler(sas_url_container=cls.__get_sas_url(container_name)).container.download_folder(
                    folder=str(blob_dir), save_at=str(local_dir)
                )
            else:
                # rm all files from local and download all from blob
                for old_file in local_dir.rglob("*"):
                    if old_file.is_file():
                        old_file.unlink()
                    else:
                        shutil.rmtree(old_file)
                # download
                BlobHandler(sas_url_container=cls.__get_sas_url(container_name)).container.download_folder(
                    folder=str(blob_dir), save_at=str(local_dir)
                )

    @staticmethod
    def __load_yaml(updater_path: Path) -> dict:
        """load updater yaml file into dict"""
        yaml_file_name = "local_file_updater.yaml"
        yaml_path = updater_path / yaml_file_name
        if not yaml_path.exists():
            raise FileNotFoundError(f"File not found: '{updater_path}'")

        # load yaml file into dict
        with open(yaml_path, "r") as yaml_stream:
            yml = yaml.load_all(yaml_stream, yaml.Loader).__next__()

        return yml

    @classmethod
    def update_locals(
        cls,
        updater_path: Path,
        dot_env_path: Path = "./.env",
        only_files_in_dir: bool = False,
        keep_onlylocals: bool = False,
    ) -> None:
        """update local files according to the 'local_file_updater.yaml' file

        Parameters:
            updater_path: Path - path to the 'local_file_updater.yaml' file
            dot_env_path: Path - path to the .env file
            only_files_in_dir: bool - updates files from local dir, otherwise will remove
                old files from local dir and download all blobs from blob dir
            keep_onlylocals: bool - while updating dir if file doesnt exist in blob, keep it in local dir
        """
        # validation
        if not isinstance(updater_path, Path):
            updater_path = Path(updater_path)

        # load updater yaml
        yml = cls.__load_yaml(updater_path=updater_path)

        # try to load .env
        if dot_env_path:
            # try to load .env
            dot_env_path = Path(dot_env_path).resolve()
            if dot_env_path.exists():
                load_dotenv(dot_env_path)
            else:
                raise FileNotFoundError(f".env file not found: '{dot_env_path}'")
        else:
            # get interactively
            cls.__get_sass_interactively(yaml_dict=yml)

        # handle files
        for file in yml["files"]:
            cls.__handle_yaml_row(file, updater_path, only_files_in_dir, keep_onlylocals)

    def __str__(self):
        name = f"Container: {self.container.name}" if self.container else "Blob: {self.blob.name}"

        return f"BlobHandler <{name}>"

    def __repr__(self):
        return self.__str__()


if __name__ == "__main__":
    local_files_dir = Path("/home/anadirov/Documents/Competo/local_dwh/")
    LocalFileUpdater.update_locals(local_files_dir, dot_env_path=".env")
    # LocalFileUpdater.update_locals(local_files_dir, dot_env_path=None)
