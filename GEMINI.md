# Project Instructions & Directives

## Workflow & Git Guidelines
- **Automatic Commits**: After completing any task, feature implementation, or bugfix successfully (and ensuring all tests pass), generate a clear and descriptive git commit of the changes with appropriate author attribution.
- **Test Integrity**: Ensure pytest test suite passes before committing changes.

## Security & Secrets Management
- **No Hardcoded Secrets**: Never hardcode credentials, passwords, API tokens, access secrets, private keys, or certificates in source files, scripts, or configuration files that could cause an access or data breach.
- **Parametrize Sensitive Data**: Always parametrize all sensitive values using environment variables, command-line arguments (with secure defaults/fallbacks), or external secret injection mechanisms.
