from flask import render_template, request, redirect, url_for
from werkzeug.security import generate_password_hash

from database import (
    create_user, get_all_users, delete_user_by_id,
    get_all_device_configs, toggle_device_status, 
    add_asset, get_all_assets, assign_device_to_asset,
    get_assets_with_devices
)

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
    @app.route("/admin/devices")
    def admin_devices():
        check = require_role("admin")
        if check:
            return check

        devices = get_all_device_configs()

        return render_template("devices.html", devices=devices)
    
    @app.route("/admin/devices/add", methods=["POST"])
    def add_device():

        check = require_role("admin")
        if check:
            return check

        from database import add_device_config

        add_device_config(
            request.form["site"],
            request.form["device_name"],
            request.form["device_type"],
            request.form["device_eui"],
            request.form["protocol"],
            request.form["host"],
            int(request.form["port"]),
            int(request.form["start_register"]),
            int(request.form["register_count"]),
            int(request.form["polling_interval"])
        )

        return redirect("/admin/devices")

    @app.route("/admin/devices/toggle/<int:device_id>")
    def toggle_device(device_id):
        check = require_role("admin")
        if check:
            return check

        toggle_device_status(device_id)

        return redirect("/admin/devices")


    @app.route("/delete_user/<int:user_id>")
    def delete_user(user_id):
        check = require_role("admin")
        if check:
            return check

        delete_user_by_id(user_id)

        return redirect(url_for("admin_users"))
    

    @app.route("/admin/assets", methods=["GET", "POST"])
    def admin_assets():
        check = require_role("admin")
        if check:
            return check

        if request.method == "POST":
            add_asset(
                request.form["site"],
                request.form["asset_name"],
                request.form["asset_type"],
                request.form["description"]
            )

            return redirect("/admin/assets")

        assets = get_assets_with_devices()

        return render_template("assets.html", assets=assets)
    


    @app.route("/admin/assets/assign", methods=["POST"])
    def assign_asset_device():

        check = require_role("admin")
        if check:
            return check

        assign_device_to_asset(
            request.form["asset_id"],
            request.form["device_eui"]
        )

        return redirect("/admin/assets")
    


