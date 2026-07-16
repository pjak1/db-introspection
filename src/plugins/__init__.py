"""Read-only SDK and loader for opt-in capability plugins.

This package contains NO mutation logic. It only defines the stable contract
(`PluginContext`, envelope re-exports, and write-permission helpers) that a
manually installed plugin binds to, plus a loader that is inert unless explicitly
enabled. Plugins may register any additional tools; the write/DDL plugin is the
primary use case and its per-connection permission gate lives here.
"""
