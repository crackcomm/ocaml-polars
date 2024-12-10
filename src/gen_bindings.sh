#!/usr/bin/env bash

# Write to file: types.ml

cat <<EOF >types.ml
(* Copyright © 2024 Łukasz Kurowski. All rights reserved.
   SPDX-License-Identifier: MIT *)

open! Core

EOF

rust_to_ocaml \
  --no-header \
  --config rust_to_ocaml.toml \
  types.rs >>types.ml

# run ocamlformat if installed
which ocamlformat && ocamlformat -i types.ml
