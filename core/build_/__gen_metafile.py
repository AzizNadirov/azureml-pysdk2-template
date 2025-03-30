"""generates file-config with meta info about project."""

import subprocess
from datetime import datetime
from pathlib import Path

import yaml

from core.settings import settings


def get_git_config() -> dict:
    """Returns dict of repo config."""
    try:
        username = subprocess.check_output(["git", "config", "user.name"]).decode("utf-8").strip()
        email = subprocess.check_output(["git", "config", "user.email"]).decode("utf-8").strip()
        remote_url = subprocess.check_output(["git", "ls-remote", "--get-url", "origin"]).decode("utf-8").strip()
    except Exception:
        username = "unknown"
        email = "unknown"
        remote_url = "unknown"
    return {"username": username, "email": email, "remote_url": remote_url}


def get_infofile_content(
    pipeline_name: str,
    pipeline_description: str,
    experiment_name: str,
    **kwargs,
):
    """Generate yaml file with meta info about project."""
    git_info = get_git_config()
    yaml_dict = {
        "pipeline_name": pipeline_name,
        "pipeline_description": pipeline_description,
        "experiment_name": experiment_name,
        "git": {
            "username": git_info["username"],
            "email": git_info["email"],
            "remote_url": git_info["remote_url"],
        },
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    if kwargs:
        yaml_dict.update(kwargs)

    return yaml.dump(yaml_dict, indent=4)


def build_infofile(
    pipeline_name: str,
    pipeline_description: str,
    experiment_name: str,
    save_at: str | Path | None = None,
    **kwargs,
) -> None:
    """Creates yaml file with meta info about project.

    Parameters
    ----------
        - pipeline_name: str - name of the pipeline
        - pipeline_description: str - description of the pipeline
        - experiment_name: str - name of the experiment
        - save_at: Optional[Union[str, Path]] - path to save the yaml file. Default - root of the project
        - kwargs: dict - additional arguments for include to the file.
    """
    yaml_content = get_infofile_content(
        pipeline_name=pipeline_name,
        pipeline_description=pipeline_description,
        experiment_name=experiment_name,
        **kwargs,
    )
    if save_at:
        save_at = Path(save_at)
        if not save_at.exists():
            msg = f"Directory not found: '{save_at}'"
            raise ValueError(msg)

        if save_at.is_file():
            msg = f"'save_at' must be a directory: '{save_at}'"
            raise ValueError(msg)

        save_as = save_at / settings.DEFAULT_INFOFILE_NAME
        with open(save_as, "w") as f:
            f.write(yaml_content)
    else:
        save_as = settings.BASE_DIR / settings.DEFAULT_INFOFILE_NAME
        with open(save_as, "w") as f:
            f.write(yaml_content)
