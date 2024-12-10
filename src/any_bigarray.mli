(* Copyright © 2023 Łukasz Kurowski. All rights reserved.
   SPDX-License-Identifier: MIT *)

open Types
open Bigarray

(** Data type that can be represented both in OCaml and in Rust, Polars. *)
type t =
  | Boolean of (int, int8_unsigned_elt, c_layout) Array1.t
  | Int64 of (nativeint, nativeint_elt, c_layout) Array1.t
  | Float32 of (float, float32_elt, c_layout) Array1.t
  | Float64 of (float, float64_elt, c_layout) Array1.t
[@@deriving typed_variants]

(** [int64_exn v] extracts the integer value from [t] variant of type [Int64], raising an
    exception if the variant is of a different type. *)
val int64_exn
  :  t
  -> (nativeint, Bigarray.nativeint_elt, Bigarray.c_layout) Bigarray.Array1.t

(** [float32_exn v] extracts the float value from [t] variant of type [Float32], raising
    an exception if the variant is of a different type. *)
val float32_exn : t -> (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array1.t

(** [float64_exn v] extracts the float value from [t] variant of type [Float64], raising
    an exception if the variant is of a different type. *)
val float64_exn : t -> (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t

(** [bool_exn v] extracts the boolean value from [t] variant of type [Boolean], raising an
    exception if the variant is of a different type. *)
val bool_exn : t -> (int, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t

(** [equal t1 t2] returns true if [t1] and [t2] are equal. *)
val equal : t -> t -> bool

(** [length t] returns the length of the bigarray in [t]. *)
val length : t -> int

(** [is_empty t] returns true if the bigarray in [t] is empty. *)
val is_empty : t -> bool

(** [get t ~idx] returns the value at index [idx] in the bigarray in [t]. *)
val get : t -> idx:int -> any_value
