from flask import Flask

app = Flask(__name__)  # Flask app object

@app.get("/old-home")
def home():
    return "Hello, Flask! 🐍"

from flask import request

# http://127.0.0.1:5000/greet/Juan?excited=1
@app.get("/greet/<name>")
def greet(name):
    excited = request.args.get("excited", "0") == "1"
    suffix = "!!!" if excited else "."
    return f"Hi {name}{suffix}"

from flask import render_template

@app.get("/user/<name>")
def user_profile(name):
    hobbies = ["coding", "running", "music"]
    return render_template("user.html", name=name, hobbies=hobbies)

from flask import jsonify

@app.post("/api/sum")
def api_sum():
    data = request.get_json()
    a = data["a"]
    b = data["b"]
    return jsonify({"result": a + b})

from flask import url_for, redirect

@app.get("/")
def index():
    # Generate URLs by function name (safer than hard-coding strings)
    return f'<a href="{url_for("dashboard")}">Go to dashboard</a>'

@app.get("/dashboard")
def dashboard():
    return "Dashboard"

@app.get("/old-dashboard")
def old_dashboard():
    # Permanent redirect to new endpoint
    return redirect(url_for("dashboard"), code=301)

if __name__ == "__main__":
    # For local development only (not production)
    app.run(debug=True)