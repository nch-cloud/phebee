# Constants for testing

TEST_PROJECT_ID = "test-project"
TEST_PROJECT_LABEL = "test-project-label"
TEST_PROJECT2_ID = "test-project2"
TEST_PROJECT2_LABEL = "test-project2-label"

# Parameterized configs for different projects / test modules
PROJECT_CONFIGS = {
    "phenopacket": {
        "PROJECT_ID": "phenopacket-test",
        "PROJECT_LABEL": "phenopacket-test-label",
        "ZIP_PATH": "tests/integration/data/phenopackets/sample_phenopackets.zip",
        "EXPECTED_PACKET_COUNT": 4,
    }
}
