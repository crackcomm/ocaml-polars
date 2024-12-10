(* Copyright © 2023 Łukasz Kurowski. All rights reserved.
   SPDX-License-Identifier: MIT *)

open Core
open Bigarray
open Polars

module Print_bigarray : sig
  val print_uint8 : (int, int8_unsigned_elt, 'layout) Array1.t -> unit
  val print_float : (float, 'elt, 'layout) Array1.t -> unit
  val print_nativeint : (nativeint, nativeint_elt, 'layout) Array1.t -> unit
end = struct
  let print (type layout) ~f (ba : ('a, 'b, layout) Array1.t) =
    let offset =
      match Array1.layout ba with
      | Bigarray.C_layout -> 0
      | Bigarray.Fortran_layout -> 1
    in
    printf "[";
    let len = Array1.dim ba - 1 in
    for i = 0 to len do
      let num = sprintf f ba.{i + offset} in
      if i = len then printf "%s]\n" num else printf "%s " num
    done
  ;;

  let print_uint8 (type layout) (ba : (int, int8_unsigned_elt, layout) Array1.t) =
    print ~f:"%d" ba
  ;;

  let print_float (type layout elt) (ba : (float, elt, layout) Array1.t) =
    print ~f:"%.4f" ba
  ;;

  let print_nativeint (type layout) (ba : (nativeint, nativeint_elt, layout) Array1.t) =
    print ~f:"%nd" ba
  ;;
end

let select_bigarray df ~col = select_by_name_exn df ~col |> Series.to_bigarray_exn

let print_f32_series series =
  Any_bigarray.float32_exn series |> Print_bigarray.print_float
;;

let print_f64_series series =
  Any_bigarray.float64_exn series |> Print_bigarray.print_float
;;

let print_col_i64 df ~col =
  select_bigarray df ~col |> Any_bigarray.int64_exn |> Print_bigarray.print_nativeint
;;

let print_col_f32 df ~col = select_bigarray df ~col |> print_f32_series
