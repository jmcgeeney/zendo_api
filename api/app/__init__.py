from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

from app.config import config_by_name

db = SQLAlchemy()
migrate = Migrate()


def create_app(config_name: str = "development") -> Flask:
    app = Flask(__name__)
    app.config.from_object(config_by_name[config_name])

    db.init_app(app)
    migrate.init_app(app, db)

    from app.api import api_bp
    app.register_blueprint(api_bp, url_prefix="/api")

    @app.route("/health")
    def health_check():
        return {"status": "ok"}, 200

    return app
