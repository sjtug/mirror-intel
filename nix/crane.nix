{
  craneLib,
  pkgs,
  lib,
  ...
}:
rec {
  src = craneLib.cleanCargoSource ../.;
  commonArgs = {
    inherit src;
    strictDeps = true;

    nativeBuildInputs = with pkgs; [ pkg-config ];

    env.OPENSSL_NO_VENDOR = 1;

    buildInputs = with pkgs; [ openssl ];

    checkFlags = [
      # require network
      "--skip=repos::tests::test_get_head::case_1"
    ];

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
