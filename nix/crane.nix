{
  craneLib,
  pkgs,
  ...
}:
let
  inherit (pkgs) cacert;

  src = craneLib.cleanCargoSource ../.; # TODO: decouple with filepath
  commonArgs = {
    inherit src;
    strictDeps = true;

    nativeBuildInputs = [ cacert ];

    doCheck = false; # Test separately with cargo-nextest
  };

  # Build *just* the cargo dependencies, so we can reuse
  # all of that work (e.g. via cachix) when running in CI
  cargoArtifacts = craneLib.buildDepsOnly commonArgs;
in
{
  inherit src commonArgs cargoArtifacts;
}
