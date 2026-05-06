from flask import render_template, request, redirect, url_for
from werkzeug.security import generate_password_hash
from database import create_user, get_all_users, delete_user_by_id
from auth import require_role


def register_admin_routes(app):

    @app.route("/admin/users", methods=["GET", "POST"])
    def admin_users():
        check = require_role("admin")
        if check:
            return check

        if request.method == "POST":
            username = request.form.get("username")
            password = request.form.get("password")
            role = request.form.get("role")
            hashed_password = generate_password_hash(password)

            create_user(username, hashed_password, role)

        users = get_all_users()

        return render_template("users.html", users=users, message=None)


    @app.route("/delete_user/<int:user_id>")
    def delete_user(user_id):
        check = require_role("admin")
        if check:
            return check

        delete_user_by_id(user_id)

        return redirect(url_for("admin_users"))