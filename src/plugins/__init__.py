"""Read-only SDK and loader for opt-in write/DDL capability plugins.

This package contains NO mutation logic. It only defines the stable contract
(`PluginContext`, `MUTATING` annotations, envelope re-exports) that a manually
installed plugin binds to, plus a loader that is inert unless explicitly enabled.
"""
