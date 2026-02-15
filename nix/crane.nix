{
  craneLib,
  pkgs,
  lib,
  ...
}:
rec {
  src =
    let
      unfilteredRoot = ../.;
    in
    lib.fileset.toSource {
      root = unfilteredRoot;
      fileset = lib.fileset.unions [
        (craneLib.fileset.commonCargoSources unfilteredRoot)
        (lib.fileset.fileFilter (file: file.hasExt "toml") (unfilteredRoot + "/tests/config"))
        (lib.fileset.maybeMissing (unfilteredRoot + "/LICENSE"))
      ];
    };
  commonArgs = {
    inherit src;
    strictDeps = true;

    nativeBuildInputs = [ ];

    buildInputs = [ ];

    nativeCheckInputs = [ pkgs.cacert ]; # required for tests

    doCheck = false; # Run with cargo-nextest instead

    env.OPENSSL_NO_VENDOR = 1;

    meta = {
      description = "Intelligent mirror redirector middleware for SJTUG";
      homepage = "https://github.com/sjtug/mirror-intel";
      license = with lib.licenses; [ asl20 ];
      platforms = lib.platforms.linux;
      maintainers = with lib.maintainers; [ definfo ];
    };
  };

  # Build *just* the cargo dependencies, so we can reuse
  # all of that work (e.g. via cachix) when running in CI
  cargoArtifacts = craneLib.buildDepsOnly commonArgs;

  # Build the actual crate itself, reusing the dependency
  # artifacts from above.
  my-crate = craneLib.buildPackage (
    commonArgs
    // {
      inherit cargoArtifacts;
    }
  );
}
