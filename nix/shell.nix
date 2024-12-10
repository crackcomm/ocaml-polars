{ pkgs, polars_ocaml, rustToolchain }:

let
  rust_to_ocaml = pkgs.stdenv.mkDerivation {
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
in with pkgs;
with ocamlPackages;
mkShell {
  inputsFrom = [ polars_ocaml ];
  packages = [
    nixfmt-classic
    ocaml
    dune_3
    # language server
    ocaml-lsp
    # formatter
    ocamlformat
    # docs
    odoc
    rustToolchain
  ];

  buildInputs = [ rust_to_ocaml ];
}
