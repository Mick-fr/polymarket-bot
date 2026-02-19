"""
Authentification du dashboard.
Login par mot de passe unique hashé en bcrypt.
"""

import functools

import bcrypt
from flask import redirect, request, session, url_for


def check_password(plain: str, hashed: str) -> bool:
    """Vérifie un mot de passe en clair contre son hash bcrypt."""
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def login_required(f):
    """Décorateur : redirige vers /login si non authentifié."""

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)

    return wrapper
