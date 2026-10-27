## Installation

Calrissian is available at: [https://pypi.org/project/calrissian/](https://pypi.org/project/calrissian/)

Install it with:

```
pip install calrissian
```

### Certificate security update

For the [eMudhra / emSign CA incident](https://discuss.python.org/t/109576),
update your operating system's CA certificates and use Python 3.11.18,
3.12.16, 3.13.18, 3.14.10, or 3.15.2 (or a later patch release).
Calrissian requires certifi 2026.10.19 or newer. Existing installations must
also update pip, which carries its own certificate bundle.

From a checkout, activate the environment you want to update and run:

```sh
sh scripts/bootstrap-pip.sh
python -m pip install --upgrade 'certifi>=2026.10.19' calrissian
```

The bootstrap script verifies the pip wheel using the SHA256 hash published
in the advisory. Run it again for each newly created virtual environment,
including Hatch environments, before installing packages there. Set `PYTHON`
to the environment's Python executable if it is not named `python`.
For new Hatch environments, set `VIRTUALENV_PIP=26.2.2` before running Hatch
so its virtualenvs are seeded with the fixed pip version.

Rebuild and redeploy existing container images to refresh OS certificates
and avoid reusing vulnerable dependency layers:

```sh
docker build --pull --no-cache -t calrissian .
```
