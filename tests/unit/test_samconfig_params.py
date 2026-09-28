"""Where the integration suite's deploy parameters come from.

The stack-deploying fixture used to carry the network parameters as literals,
which published the maintainers' VPC, subnet and log bucket names and pointed
every outside reader's test deploy at their network. It now reads them from the
reader's own samconfig.yaml.

`sam deploy --parameter-overrides` replaces the config file's list instead of
merging into it, so a parameter the loader drops is a parameter the deploy loses
silently -- the stack still builds, with a template default in place of the value
the config asked for. test_supplies_every_parameter_the_literals_did pins the
full set against what the hardcoded list used to pass.

These tests run offline: no samconfig.yaml in the working tree is read, and the
values below are placeholders.
"""
import os
import sys
import textwrap

import pytest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "integration"),
)

from samconfig_params import (  # noqa: E402
    OVERRIDES_ENV_VAR,
    SamConfigError,
    load_parameter_overrides,
    parse_parameter_overrides,
)


# The shape of samconfig.yaml's integration-test environment, with the four
# site-specific values replaced by placeholders.
SAMCONFIG = """
    version: 0.1
    integration-test:
      deploy:
        parameters:
          stack_name: phebee-integration-test
          parameter_overrides:
            - AppName=phebee-it
            - VpcId=vpc-000000000000test
            - SubnetId1=subnet-00000000000test1
            - SubnetId2=subnet-00000000000test2
            - S3AccessLogBucketName=example-access-logs
            - RunOntologyUpdatesOnSchedule=false
            - CreateEvidenceTableFlag=true
          tags:
            - app=phebee
    dev:
      deploy:
        parameters:
          parameter_overrides:
            - AppName=phebee-dev
            - VpcId=vpc-000000000000test
            - SubnetId1=subnet-00000000000test1
            - SubnetId2=subnet-00000000000test2
"""


def write_config(directory, body=SAMCONFIG, filename="samconfig.yaml"):
    path = os.path.join(str(directory), filename)
    with open(path, "w") as config_file:
        config_file.write(textwrap.dedent(body))
    return path


def load(directory, config_env="integration-test", environ=None):
    return load_parameter_overrides(
        config_env, search_dir=str(directory), environ={} if environ is None else environ
    )


def test_supplies_every_parameter_the_literals_did(tmp_path):
    """Nothing the pre-change deploy command passed has gone missing."""
    write_config(tmp_path)

    overrides = load(tmp_path)

    assert set(overrides) == {
        "AppName",
        "VpcId",
        "SubnetId1",
        "SubnetId2",
        "S3AccessLogBucketName",
        "RunOntologyUpdatesOnSchedule",
        "CreateEvidenceTableFlag",
    }
    assert overrides["VpcId"] == "vpc-000000000000test"
    assert overrides["S3AccessLogBucketName"] == "example-access-logs"


def test_appname_is_the_fixtures_to_override(tmp_path):
    """The loader returns the config's AppName; the fixture replaces it."""
    write_config(tmp_path)

    overrides = load(tmp_path)

    assert overrides["AppName"] == "phebee-it"

    overrides["AppName"] = "phebee-it-abc"
    rendered = [f"{key}={value}" for key, value in overrides.items()]
    assert "AppName=phebee-it-abc" in rendered
    assert "AppName=phebee-it" not in rendered


def test_selects_the_requested_config_env(tmp_path):
    write_config(tmp_path)

    assert load(tmp_path, config_env="dev")["AppName"] == "phebee-dev"


def test_reads_the_single_string_form():
    """SAM also accepts every override in one string, quotes included."""
    overrides = parse_parameter_overrides(
        'VpcId=vpc-1 SubnetId1=subnet-1 AppName="spaced name"', "test"
    )

    assert overrides == {
        "VpcId": "vpc-1",
        "SubnetId1": "subnet-1",
        "AppName": "spaced name",
    }


def test_value_may_contain_an_equals_sign():
    assert parse_parameter_overrides(["Filter=a=b"], "test") == {"Filter": "a=b"}


def test_empty_network_id_is_rejected(tmp_path):
    """samconfig.yaml.example ships these blank, and the template has no usable
    default -- deploying anyway would land the stack in the wrong network."""
    write_config(
        tmp_path,
        """
        version: 0.1
        integration-test:
          deploy:
            parameters:
              parameter_overrides:
                - AppName=phebee-it
                - VpcId=
                - SubnetId1=
                - SubnetId2=
        """,
    )

    with pytest.raises(SamConfigError) as excinfo:
        load(tmp_path)

    message = str(excinfo.value)
    assert "VpcId" in message and "SubnetId1" in message and "SubnetId2" in message
    assert "samconfig.yaml.example" in message


def test_absent_network_id_is_rejected(tmp_path):
    write_config(
        tmp_path,
        """
        version: 0.1
        integration-test:
          deploy:
            parameters:
              parameter_overrides:
                - AppName=phebee-it
                - VpcId=vpc-1
                - SubnetId1=subnet-1
        """,
    )

    with pytest.raises(SamConfigError, match="SubnetId2"):
        load(tmp_path)


def test_unknown_config_env_lists_the_available_ones(tmp_path):
    write_config(tmp_path)

    with pytest.raises(SamConfigError) as excinfo:
        load(tmp_path, config_env="staging")

    message = str(excinfo.value)
    assert "staging" in message
    assert "dev" in message and "integration-test" in message
    assert "version" not in message


def test_missing_config_file_explains_both_routes(tmp_path):
    with pytest.raises(SamConfigError) as excinfo:
        load(tmp_path)

    message = str(excinfo.value)
    assert "samconfig.yaml.example" in message
    assert OVERRIDES_ENV_VAR in message


def test_env_var_layers_over_the_config_file(tmp_path):
    write_config(tmp_path)

    overrides = load(
        tmp_path, environ={OVERRIDES_ENV_VAR: "VpcId=vpc-override AppName=from-env"}
    )

    assert overrides["VpcId"] == "vpc-override"
    assert overrides["AppName"] == "from-env"
    # Untouched keys survive the layering.
    assert overrides["SubnetId1"] == "subnet-00000000000test1"
    assert overrides["CreateEvidenceTableFlag"] == "true"


def test_env_var_works_without_a_config_file(tmp_path):
    """The escape hatch for a machine that has no samconfig.yaml."""
    overrides = load(
        tmp_path,
        environ={
            OVERRIDES_ENV_VAR: "VpcId=vpc-1 SubnetId1=subnet-1 SubnetId2=subnet-2"
        },
    )

    assert overrides == {
        "VpcId": "vpc-1",
        "SubnetId1": "subnet-1",
        "SubnetId2": "subnet-2",
    }


def test_entry_without_a_value_separator_is_rejected():
    """A bare value is the likely typo, and it would otherwise be dropped."""
    with pytest.raises(SamConfigError, match="VpcId"):
        parse_parameter_overrides(["VpcId"], "test")


def test_entry_with_a_non_parameter_key_is_rejected():
    with pytest.raises(SamConfigError, match="not a parameter=x"):
        parse_parameter_overrides(["not a parameter=x"], "test")


def test_unexpected_parameter_overrides_type_is_rejected():
    with pytest.raises(SamConfigError, match="dict"):
        parse_parameter_overrides({"VpcId": "vpc-1"}, "test")
