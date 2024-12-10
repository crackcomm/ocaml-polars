(* Copyright © 2023-2024 Łukasz Kurowski. All rights reserved.
   SPDX-License-Identifier: MIT *)

open Core

let () = Core_unix.putenv ~key:"POLARS_TABLE_WIDTH" ~data:"120"
let () = Core_unix.putenv ~key:"POLARS_FMT_MAX_ROWS" ~data:"8"
let () = Core_unix.putenv ~key:"TF_CPP_MIN_LOG_LEVEL" ~data:"3"
let () = Core_unix.putenv ~key:"POLARS_FMT_TABLE_FORMATTING" ~data:"ASCII_MARKDOWN"

let testdata =
  let cwd = Sys_unix.getcwd () |> Filename_unix.realpath in
  let ( / ) = Filename.concat in
  cwd / ".." / ".." / ".." / "test"
;;

let schema =
  [| "trade_id", Polars.Dtype.Int64
   ; "price", Float32
   ; "qty", Float32
   ; "first_id", Int64
   ; "last_id", Int64
   ; "timestamp", Int64
   ; "is_bid", Boolean
  |]
;;

let trades1 =
  lazy
    (Filename.concat testdata "trades-1.csv"
     |> Polars.read_csv ~schema ~has_header:false ~skip_rows:1)
;;

let trades2 =
  lazy
    (Filename.concat testdata "trades-2.csv"
     |> Polars.read_csv ~schema ~has_header:false ~skip_rows:1)
;;

let trades1 () = Lazy.force trades1
let trades2 () = Lazy.force trades2
