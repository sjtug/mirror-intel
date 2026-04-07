{ pkgs, my-crate }:
{
  e2e-simple =
    pkgs.runCommand "e2e-tests"
      {
        nativeBuildInputs = [
          my-crate
          pkgs.coreutils
          pkgs.curl
          pkgs.retry
          pkgs.cacert
        ];
        ROCKET_TOML_PATH = "Rocket.toml";
      }
      ''
        cp ${../config/Rocket.toml} Rocket.toml
        cp Rocket.toml mirror-intel.toml
        cp ${./simple.sh} simple.sh
        chmod u+w simple.sh
        chmod +x simple.sh
        patchShebangs --build simple.sh
        ./simple.sh
      '';
}
