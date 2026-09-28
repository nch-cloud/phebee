"""Deploy parameters for the integration suite, read from the reader's samconfig.

`sam deploy --parameter-overrides` *replaces* the config file's
parameter_overrides list rather than merging into it. The stack-deploying fixture
has to override AppName -- each test stack needs its own database -- so it also
has to re-supply every other parameter it wants kept. Those values used to be
literals in conftest.py, which disclosed the maintainers' VPC, subnet and log
bucket names and meant the documented "deploy and test" path deployed into their
network no matter whose samconfig.yaml was present.

Paths resolve against the current directory, which is where `sam deploy` itself
looks for samconfig.yaml, so the fixture reads the same file SAM will read.
"""

import os
import shlex

try:
    import yaml
except ImportError:  # pragma: no cover - surfaced as a SamConfigError below
    yaml = None


CONFIG_FILENAMES = ("samconfig.yaml", "samconfig.yml")

# The template declares no usable default for these, so an empty value deploys a
# broken stack. Treat it as a configuration error instead of passing it through.
REQUIRED_PARAMS = ("VpcId", "SubnetId1", "SubnetId2")

OVERRIDES_ENV_VAR = "PHEBEE_TEST_PARAM_OVERRIDES"

_EXAMPLE_HINT = (
    "Copy samconfig.yaml.example to samconfig.yaml and fill in the values for "
    f"your account, or set {OVERRIDES_ENV_VAR} to a space-separated list of "
    "Key=Value pairs."
)


class SamConfigError(Exception):
    """samconfig.yaml is missing, malformed, or missing a required value."""


def find_config_file(search_dir=None):
    """Return the path to samconfig.yaml in search_dir, or None if absent."""
    search_dir = os.getcwd() if search_dir is None else search_dir

    for filename in CONFIG_FILENAMES:
        candidate = os.path.join(search_dir, filename)
        if os.path.exists(candidate):
            return candidate

    return None


def parse_parameter_overrides(raw, source):
    """Parse a samconfig parameter_overrides value into a dict.

    SAM accepts either a list of "Key=Value" strings or a single string holding
    them all, so both forms are handled. `source` only appears in error messages.
    """
    if raw is None:
        return {}

    if isinstance(raw, str):
        tokens = shlex.split(raw)
    elif isinstance(raw, (list, tuple)):
        tokens = list(raw)
    else:
        raise SamConfigError(
            f"parameter_overrides in {source} is a {type(raw).__name__}; "
            "expected a list of Key=Value strings or a single string of them."
        )

    overrides = {}
    for token in tokens:
        key, separator, value = str(token).partition("=")
        if not separator or not key.isalnum():
            raise SamConfigError(
                f"Could not read {token!r} in {source} as a CloudFormation "
                "parameter. Entries must be written Key=Value, one parameter "
                "per entry."
            )
        overrides[key] = value

    return overrides


def load_parameter_overrides(config_env, search_dir=None, environ=None):
    """Collect the deploy parameters for a samconfig environment.

    Values come from samconfig.yaml's `deploy.parameters.parameter_overrides`
    for `config_env`, with anything in $PHEBEE_TEST_PARAM_OVERRIDES layered on
    top so a machine without a samconfig.yaml can still run the suite.

    Raises SamConfigError if no source supplies the network parameters, since
    the alternative is deploying into the wrong VPC.
    """
    environ = os.environ if environ is None else environ

    overrides = {}
    config_path = find_config_file(search_dir)

    if config_path is not None:
        if yaml is None:
            raise SamConfigError(
                f"Reading {config_path} requires PyYAML, which is not "
                "installed. Install it with: pip install pyyaml"
            )

        with open(config_path, "r") as config_file:
            config = yaml.safe_load(config_file) or {}

        if not isinstance(config, dict):
            raise SamConfigError(f"{config_path} does not contain a YAML mapping.")

        if config_env not in config:
            available = sorted(key for key in config if key != "version")
            raise SamConfigError(
                f"Config environment {config_env!r} is not defined in "
                f"{config_path}. Available environments: "
                f"{', '.join(available) or 'none'}."
            )

        parameters = (
            config[config_env].get("deploy", {}).get("parameters", {})
            if isinstance(config[config_env], dict)
            else {}
        )
        overrides = parse_parameter_overrides(
            parameters.get("parameter_overrides"),
            f"{config_path} [{config_env}]",
        )

    env_overrides = environ.get(OVERRIDES_ENV_VAR)
    if env_overrides:
        overrides.update(
            parse_parameter_overrides(env_overrides, f"${OVERRIDES_ENV_VAR}")
        )

    if not overrides:
        location = config_path or "samconfig.yaml"
        raise SamConfigError(
            f"No deploy parameters found for config environment "
            f"{config_env!r} in {location}. {_EXAMPLE_HINT}"
        )

    missing = [name for name in REQUIRED_PARAMS if not overrides.get(name)]
    if missing:
        location = config_path or f"${OVERRIDES_ENV_VAR}"
        raise SamConfigError(
            f"{', '.join(missing)} must be set for config environment "
            f"{config_env!r} in {location}, otherwise the test stack deploys "
            f"into the wrong network. {_EXAMPLE_HINT}"
        )

    return overrides
