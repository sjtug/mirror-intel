# mirror-intel

The intelligent mirror redirector middleware for SJTUG.

## Usage

First of all, put `Roclet.toml` in the same folder as `mirror-intel`. Then,

```
RUST_LOG=info ./mirror-intel
```

For more advanced usage, you may refer to `mirror-intel` service defined in [mirror-docker-siyuan(https://github.com/sjtug/mirror-docker-siyuan).

After starting `mirror-intel`, it will serve on HTTP port 8000. You may set package manager with `localhost:8000` endpoint,
and start testing.

## Supported Repos

* rustup
* homebrew-bottles
* pypi-packages
* fedora-iot
* fedora-ostree
* flathub
* crates-io
* dart-pub
* guix

## Configuration

Please refer to `Rocket.toml` for more information.

## Detail

* mirror-intel will first query if object exists in s3 backend
* if yes, it will redirect user to s3 object storage
* if not, it will redirect user to original site, and submit task for download
* the task will download file from original site and upload it to s3 backend
