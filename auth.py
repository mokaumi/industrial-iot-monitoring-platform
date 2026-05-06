from flask import render_template, request, session, redirect, url_for
from werkzeug.security import check_password_hash
from database import get_user_by_username


def require_role(*allowed_roles):
    if "user" not in session:
        return redirect(url_for("login"))

    if session.get("role") not in allowed_roles:
        return "Access denied", 403

    return None


def register_auth_routes(app):

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = request.form.get("username")
            password = request.form.get("password")

            user = get_user_by_username(username)

            if user and check_password_hash(user[2], password):
                session["user"] = username
                session["role"] = user[3]
                return redirect(url_for("dashboard"))

            return "Invalid username or password"

        return render_template("login.html")


    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))