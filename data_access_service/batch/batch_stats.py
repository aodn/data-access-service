
# when getting remote job definition json, there will be some fields that are not needed for registering a job definition.
# So they should be ignored when comparing the local and remote job definitions.
batch_json_settings_ignored_field = ["root['jobDefinitionArn']", "root['revision']", "root['status']", "root['containerOrchestrationType']"]