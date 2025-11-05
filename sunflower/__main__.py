def main() -> None:  # pragma: no cover
    # I've done all of my testing on this.
    from functools import partial
    from sys import argv

    from sanic import Sanic
    from sanic.worker.loader import AppLoader

    from sunflower.infrastructure.argparser import parse_args
    from sunflower.infrastructure.sanic.bootstrap import create_app
    from sunflower.infrastructure.sanic.config import SunflowerConfig

    sunflower_config = SunflowerConfig()

    args = parse_args(argv[1:])
    sunflower_config.update_with_args(args)

    loader = AppLoader(factory=partial(create_app, sunflower_config))
    app = (  # pyright: ignore[reportUnknownVariableType]
        loader.load()  # pyright: ignore[reportUnknownMemberType]
    )

    app.prepare(  # pyright: ignore[reportUnknownMemberType]
        sunflower_config.HOST,
        sunflower_config.PORT,
        debug=sunflower_config.DEBUG,
        single_process=True,
    )

    Sanic.serve(app, app_loader=loader)  # pyright: ignore[reportUnknownMemberType]


if __name__ == "__main__":  # pragma: no cover
    main()
