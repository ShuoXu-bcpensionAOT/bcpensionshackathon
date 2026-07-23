"""Key Vault secret access (uses the running identity's token)."""
from .runtime import KEY_VAULT_URL, notebookutils


def get_secret(name):
    """Read a secret from the Key Vault named by the cp_vars `key_vault_url` variable
    (uses the running identity's token — grant it KV 'get' on that vault)."""
    if not name:
        return None
    if not KEY_VAULT_URL:
        raise Exception("cp_vars.key_vault_url is not set — cannot resolve secret " + str(name))
    return notebookutils.credentials.getSecret(KEY_VAULT_URL, name)
