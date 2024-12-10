(* Copyright © 2023 Łukasz Kurowski. All rights reserved.
   SPDX-License-Identifier: MIT *)

open Core
open Bigarray
open Types

type t =
  | Boolean of (int, int8_unsigned_elt, c_layout) Array1.t
  | Int64 of (nativeint, nativeint_elt, c_layout) Array1.t
  | Float32 of (float, float32_elt, c_layout) Array1.t
  | Float64 of (float, float64_elt, c_layout) Array1.t
[@@deriving typed_variants]

(* nativeint is i64 on rust side *)
let () = assert (Stdlib.Sys.word_size = 64)

let fail_with_expected s t =
  failwiths ~here:[%here] s (Typed_variant.which t) Typed_variant.Packed.sexp_of_t
;;

let int64_exn = function
  | Int64 v -> v
  | t -> fail_with_expected "expected int64 series" t
;;

let float32_exn = function
  | Float32 v -> v
  | t -> fail_with_expected "expected float32 series" t
;;

let float64_exn = function
  | Float64 v -> v
  | t -> fail_with_expected "expected float64 series" t
;;

let bool_exn = function
  | Boolean v -> v
  | t -> fail_with_expected "expected bool series" t
;;

let equal : t -> t -> bool = Stdlib.( = )

let length = function
  | Boolean v -> Array1.dim v
  | Int64 v -> Array1.dim v
  | Float32 v -> Array1.dim v
  | Float64 v -> Array1.dim v
;;

let is_empty t = length t = 0

let get t ~idx : any_value =
  match t with
  | Boolean v -> Boolean Int.(v.{idx} = 1)
  | Int64 v -> Int64 (Nativeint.to_int_exn v.{idx})
  | Float32 v -> Float32 v.{idx}
  | Float64 v -> Float64 v.{idx}
;;
