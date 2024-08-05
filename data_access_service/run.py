from data_access_service import create_app

app = create_app()


def main() -> None:
    app.run(debug=app.config["DEBUG"])


if __name__ == "__main__":
    main()
