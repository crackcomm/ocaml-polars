(* Copyright © 2023-2024 Łukasz Kurowski. All rights reserved.
   SPDX-License-Identifier: MIT *)

open Core
open Async
include Polars0
include Types

module Any_value = struct
  type t = any_value [@@deriving compare, equal, sexp]

  let int64_exn : t -> int = function
    | Int64 v | Datetime (v, _) -> v
    | v -> failwiths ~here:[%here] "int64_exn" v sexp_of_t
  ;;

  let float32_exn : t -> float = function
    | Float32 v -> v
    | v -> failwiths ~here:[%here] "float32_exn" v sexp_of_t
  ;;

  let bool_exn : t -> bool = function
    | Boolean v -> v
    | v -> failwiths ~here:[%here] "bool_exn" v sexp_of_t
  ;;
end

module Any_bigarray = Any_bigarray

module Dtype = struct
  type t = data_type =
    | Int64
    | Float32
    | Float64
    | Boolean
    | Datetime of time_unit
  [@@deriving compare, equal, sexp]
end

module Series = struct
  type t

  external create : string -> Any_bigarray.t -> copy:bool -> t = "ml_series_create"

  let create ~copy name arr = create name arr ~copy

  external get_exn : t -> idx:int -> any_value = "ml_series_get"
  external length : t -> int = "ml_series_length"
  external name : t -> string = "ml_series_name"

  let is_empty t = length t = 0

  let sexp_of_t t =
    Sexp.List [ Sexp.Atom "Series"; Sexp.Atom (name t); sexp_of_int (length t) ]
  ;;

  external to_bigarray_exn : t -> Any_bigarray.t = "ml_series_bigarray"
  external sum : t -> float = "ml_series_sum"
  external set_sorted_flag : t -> flag:is_sorted -> unit = "ml_series_set_sorted_flag"
  external cast : t -> dtype:data_type -> t = "ml_series_cast"
  external sub : t -> pos:int -> len:int -> t = "ml_series_slice"
  external rechunk : t -> t = "ml_series_rechunk"
  external dtype : t -> data_type = "ml_series_dtype"
  external null_count : t -> int = "ml_series_null_count"
  external multiply : t -> any_value -> t = "ml_series_multiply"

  let ( * ) = multiply
end

type t

external create : (string * Any_bigarray.t) array -> bool -> t = "ml_df_create"

let create ~copy arr = create arr copy

external equal : t -> t -> bool = "ml_df_equal"
external print : t -> unit = "ml_df_print"
external length : t -> int = "ml_df_height"
external width : t -> int = "ml_df_width"
external shape : t -> int * int = "ml_df_shape"

let is_empty df = length df = 0

external sub : t -> pos:int -> len:int -> t = "ml_df_slice"
external get_exn : t -> col:int -> idx:int -> any_value = "ml_df_get"
external get_row_exn : t -> idx:int -> any_value array = "ml_df_get_row"
external get_by_name_exn : t -> col:string -> idx:int -> any_value = "ml_df_get_by_name"

(* sorting *)
external sort : t -> string array -> bool -> bool -> t = "ml_df_sort"

let sort_cols ?(descending = false) ?(maintain_order = false) df ~cols =
  sort df cols descending maintain_order
;;

let sort ?descending ?maintain_order df ~col =
  sort_cols df ~cols:[| col |] ?descending ?maintain_order
;;

external sort_in_place : t -> string array -> bool -> bool -> unit = "ml_df_sort_in_place"

let sort_cols_in_place ?(descending = false) ?(maintain_order = false) df ~cols =
  sort_in_place df cols descending maintain_order
;;

let sort_in_place ?descending ?maintain_order df ~col =
  sort_cols_in_place df ~cols:[| col |] ?descending ?maintain_order
;;

module Duration = struct
  type t = duration [@@deriving sexp]

  let zero = Slots 0
  let of_int_ms ms = Slots (ms * 1000 * 1000)
  let of_int_sec sec = of_int_ms (sec * 1000)
end

external upsample_stable
  :  t
  -> by:string array option
  -> time_column:string
  -> every:Duration.t
  -> offset:Duration.t
  -> t
  = "ml_df_upsample_stable"

external upsample
  :  t
  -> by:string array option
  -> time_column:string
  -> every:Duration.t
  -> offset:Duration.t
  -> t
  = "ml_df_upsample"

let upsample ?(maintain_order = false) ?by ?(offset = Duration.zero) df =
  let upsample = if maintain_order then upsample_stable else upsample in
  upsample df ~by ~offset
;;

external column_names : t -> string array = "ml_df_column_names"

external rename_in_place
  :  t
  -> col:string
  -> name:string
  -> unit
  = "ml_df_rename_in_place"

(* select *)
external select : t -> string array -> t = "ml_df_select"
external drop : t -> string array -> t = "ml_df_drop"
external select_by_idx : t -> col:int -> Series.t option = "ml_df_select_by_idx"
external select_by_name : t -> col:string -> Series.t option = "ml_df_select_by_name"

let select_by_idx_exn t ~col =
  select_by_idx ~col t |> Option.value_exn ~here:[%here] ~message:"select_by_idx_exn"
;;

let select_by_name_exn t ~col =
  select_by_name ~col t |> Option.value_exn ~here:[%here] ~message:"select_by_name_exn"
;;

let null_counts t =
  Array.init (width t) ~f:(fun col -> select_by_idx_exn t ~col |> Series.null_count)
;;

let null_count t = Array.reduce (null_counts t) ~f:Int.( + ) |> Option.value ~default:0

external with_column : t -> col:Series.t -> t = "ml_df_with_column"

let cast t ~col ~dtype =
  let series = select_by_name_exn t ~col in
  if Dtype.equal (Series.dtype series) dtype
  then t
  else (
    let series = Series.cast ~dtype series in
    with_column t ~col:series)
;;

external series_bigarrays_exn
  :  t
  -> (string * Any_bigarray.t) array
  = "ml_df_series_bigarrays"

module Field = struct
  type t = string * data_type [@@deriving compare, equal, sexp]
end

module Schema = struct
  type t = Field.t array [@@deriving compare, equal, sexp]
end

module Read_csv = struct
  type t =
    { skip_rows : int
    ; has_header : bool
    ; columns : string array option
    ; schema : Schema.t option
    ; threads : int
    }
  [@@deriving sexp]
end

external read_csv : string -> Read_csv.t -> t = "ml_df_read_csv"

let read_csv ?(skip_rows = 0) ?(has_header = false) ?columns ?schema ?(threads = 1) path =
  let opts = Read_csv.{ skip_rows; has_header; columns; schema; threads } in
  read_csv path opts
;;

let read_csv' ?skip_rows ?has_header ?columns ?schema ?threads path =
  In_thread.run (fun () -> read_csv ?skip_rows ?has_header ?columns ?schema ?threads path)
;;

external read_parquet
  :  string
  -> rechunk:bool
  -> parallel:bool
  -> t
  = "ml_df_read_parquet"

external write_parquet : t -> string -> int = "ml_df_write_parquet"

let write_parquet' t f = In_thread.run (fun () -> write_parquet t f)

let read_parquet ?(rechunk = false) ?(parallel = false) path =
  read_parquet ~rechunk ~parallel path
;;

let read_parquet' ?rechunk ?parallel path =
  [%log.global.debug
    "Polars.read_parquet'"
      (path : string)
      (rechunk : bool option)
      (parallel : bool option)];
  In_thread.run (fun () -> read_parquet ?rechunk ?parallel path)
;;

let sexp_of_t t =
  Sexp.List [ Sexp.Atom "DataFrame"; sexp_of_pair sexp_of_int sexp_of_int (shape t) ]
;;

external filter_col_by_name
  :  t
  -> col:string
  -> comparison
  -> any_value
  -> t
  = "ml_df_filter_col_by_name"

external filter_col_by_name_multi
  :  t
  -> (string * comparison * any_value) array
  -> t
  = "ml_df_filter_col_by_name_multi"

let () = assert (Sys.word_size = 64)
