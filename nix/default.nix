{ pkgs, nix-filter }:

let inherit (pkgs) lib stdenv ocamlPackages rustPlatform;

in with ocamlPackages;
let
  rustSource = with nix-filter.lib;
    filter {
      root = ./..;
      include = [ "Cargo.toml" "Cargo.lock" "src/Cargo.toml" (matchExt "rs") ];
    };
  projectSource = with nix-filter.lib;
    filter {
      root = ./..;
      include =
        [ "dune-project" "src" "dune" "Cargo.toml" "Cargo.lock" "test" ];
      exclude = [ "target" "node_modules" "_build" ];
    };
in let
  rust_to_ocaml = stdenv.mkDerivation {
    pname = "rust-to-ocaml";
    version = "ef41f18";

    src = pkgs.fetchurl {
      url =
        "https://github.com/crackcomm/ocamlrep/releases/download/ef41f18/rust_to_ocaml-linux-amd64";
      sha256 = "sha256-pLhTCWXkoIVxj+bScNp6qDCycAPAKD1596e6bkAQShs=";
    };

    phases = [ "installPhase" ];
    installPhase = ''
      mkdir -p $out/bin
      install -m 755 $src $out/bin/rust_to_ocaml
    '';
  };

  polars = rustPlatform.buildRustPackage {
    pname = "polars-ocaml";
    version = "0.0.1";
    src = rustSource;
    cargoHash = lib.fakeSha256;
    nativeBuildInputs = [ rust_to_ocaml ocaml ];
    cargoLock = {
      lockFile = ../Cargo.lock;
      outputHashes = {
        "ocaml-1.0.0-beta.5" =
          "sha256-oUU7Sdo6kvRmV8/3lebIoDJjIGKiAmwOYNIwuv8Nu1s=";
        "ocamlrep-0.1.0" =
          "sha256-D4S97MkifbMyZVl5Q+DeN06Z6C/Gf3zq746RVNsxGWQ=";
      };
    };
    doCheck = false;
  };
in buildDunePackage rec {
  pname = "polars";
  version = "0.0.0-dev";

  src = projectSource;

  nativeBuildInputs = [ rust_to_ocaml ];

  propagatedBuildInputs =
    [ async core dune-configurator ppx_typed_fields polars ] ++ checkInputs;

  checkInputs = [ expect_test_helpers_async ];

  OCAML_POLARS_STUBS = polars;
}
