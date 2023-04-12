from flask import Flask, request, jsonify, render_template, Blueprint




def create_app(config=None):
    app = Flask(__name__)
    if config is not None:
        app.config.update(config)
    from section4_main.predict.app import app_bp
    app.register_blueprint(app_bp)
    
    return app