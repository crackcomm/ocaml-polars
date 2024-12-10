{ pkgs, ocaml-polars }:

with pkgs;
with ocamlPackages;
mkShell {
  inputsFrom = [ ocaml-polars ];
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
    # ocaml-index
  ];
}
