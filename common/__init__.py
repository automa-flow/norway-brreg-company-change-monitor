"""Small, shared infrastructure primitives for the Actors in this monorepo.

Rule of thumb for what belongs here: it must already be used by (or be about to
be used by) more than one Actor, and it must be small enough to read in one
sitting. Anything domain-specific stays inside the Actor that owns it.
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
