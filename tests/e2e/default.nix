{ pkgs, my-crate }:
let
  nixCacheFixture = pkgs.writeText "mirror-intel-e2e-nix-cache-fixture" ''
    nix cache fixture
  '';
in
{
  e2e-simple =
    pkgs.runCommand "e2e-tests"
      {
        nativeBuildInputs = [
          my-crate
          pkgs.coreutils
          pkgs.curl
          pkgs.nix
          pkgs.nix-serve-ng
          pkgs.python3
          pkgs.retry
          pkgs.cacert
        ];
        NIX_CACHE_FIXTURE = nixCacheFixture;
        ROCKET_TOML_PATH = "Rocket.toml";
        AWS_ACCESS_KEY_ID = "test-access-key";
        AWS_SECRET_ACCESS_KEY = "test-secret-key";
      }
      ''
        cp ${../config/Rocket.toml} Rocket.toml
        cat > mirror-intel.toml <<EOF
        [default]
        address = "127.0.0.1"
        buffer_path = "$PWD/buffer"
        direct_stream_size_kb = 16
        download_timeout = 10
        ttl = 1
        read_only = false
        workers = 1

        [default.endpoints]
        nix_channels_store = "http://127.0.0.1:18082"
        pytorch_wheels = "http://127.0.0.1:18080/whl"

        [default.s3]
        name = "test"
        region = "test"
        endpoint = "http://127.0.0.1:18081"
        website_endpoint = "http://127.0.0.1:18081"
        bucket = "bucket"

        [default.index_crawl]
        refresh_secs = 1
        fetch_timeout_secs = 5
        max_depth = 2
        max_pages = 20
        EOF

        cp ${./simple.sh} simple.sh
        cp ${./fake_services.py} fake_services.py

        chmod u+w simple.sh
        chmod +x simple.sh
        chmod u+w fake_services.py
        chmod +x fake_services.py
        patchShebangs --build simple.sh
        patchShebangs --build fake_services.py

        python3 -m py_compile fake_services.py

        ./simple.sh
      '';
}
