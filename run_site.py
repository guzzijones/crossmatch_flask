import cross_process.site as site
import os

if __name__ == "__main__":
    app = site.create_app()
    app.secret_key = os.urandom(12)
    app.run()
