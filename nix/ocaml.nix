{ pkgs }:

with pkgs;
let ocamlPackages = ocaml-ng.ocamlPackages_5_2;
in ocamlPackages.overrideScope (_: super: { })
