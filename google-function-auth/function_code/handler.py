from flask import jsonify


def handle(request):
    """Your app logic goes here.
    This is the only file you change per project.
    """
    data = request.get_json(silent=True) or {}
    name = data.get("name", "World")
    return jsonify({"message": f"Hello, {name}!"})
