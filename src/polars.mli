(* Copyright © 2023-2024 Łukasz Kurowski. All rights reserved.
   SPDX-License-Identifier: MIT *)

open Async
include module type of Polars0
include module type of Types

module Any_value : sig
  type t = any_value [@@deriving compare, equal, sexp]

  (** [int64_exn t] extracts the integer value. and raises if not a [Int64] variant. *)
  val int64_exn : t -> int

  (** [float32_exn t] extracts the float value and raises if not a [Float32] variant. *)
  val float32_exn : t -> float

  (** [bool_exn t] extracts the boolean value and raises if not a [Boolean] variant. *)
  val bool_exn : t -> bool
end

module Any_bigarray = Any_bigarray

module Dtype : sig
  type t = data_type =
    | Int64
    | Float32
    | Float64
    | Boolean
    | Datetime of time_unit
  [@@deriving compare, equal, sexp]
end

module Field : sig
  type t = string * data_type [@@deriving compare, equal, sexp]
end

module Schema : sig
  type t = Field.t array [@@deriving compare, equal, sexp]
end

module Series : sig
  type t [@@deriving sexp_of]

  (** [create ~copy name data] creates a series from a name and bigarray. If [copy] is
      [false], the bigarray should be kept alive for the lifetime of the series. *)
  val create : copy:bool -> string -> Any_bigarray.t -> t

  (** [dtype t] returns datatype of series [t]. *)
  val dtype : t -> data_type

  (** [get_exn series ~idx] gets the element at index [idx] from the series [series] as an
      [any_value] option. *)
  val get_exn : t -> idx:int -> any_value

  (** [length series] returns the number of elements in the series [series]. *)
  val length : t -> int

  (** [is_empty series] returns true if [series] is empty. *)
  val is_empty : t -> bool

  (** [name series] returns the name of the series [series]. *)
  val name : t -> string

  (** [to_bigarray_exn series] converts the series [series] to an [Any_bigarray.t]. It is
      unsafe to call this function on not rechunked series. It only returns a view of the
      underlying series. Dataframe or series should be kept alive for the lifetime of the
      bigarray. It raises an exception if series is not rechunked. *)
  val to_bigarray_exn : t -> Any_bigarray.t

  (** [sum series] sums [series] into a float. *)
  val sum : t -> float

  (** [set_sorted_flag series ~flag] sets [is_sorted] flag on a series. *)
  val set_sorted_flag : t -> flag:is_sorted -> unit

  (** [cast series ~dtype] cast [series] to another [dtype]. *)
  val cast : t -> dtype:data_type -> t

  (** [sub t pos len] returns a sub series starting from [pos] with length [len]. *)
  val sub : t -> pos:int -> len:int -> t

  (** [rechunk series] aggregate all chunks to a contiguous array of memory. *)
  val rechunk : t -> t

  (** [null_count t] count the null values in this series. *)
  val null_count : t -> int

  (** [multiply t scalar] multiplies series by a scalar value. *)
  val multiply : t -> any_value -> t

  val ( * ) : t -> any_value -> t
end

type t [@@deriving sexp_of]

(** [create data] creates a dataframe from an array of (name, Bigarray) pairs. If [copy]
    is [false], the bigarrays should be kept alive for the lifetime of the dataframe. *)
val create : copy:bool -> (string * Any_bigarray.t) array -> t

(** [equal a b] checks if two datasets [a] and [b] are equal. *)
val equal : t -> t -> bool

(** [print t] prints the dataframe [t]. *)
val print : t -> unit

(** [length t] returns the number of rows in the dataframe [t]. *)
val length : t -> int

(** [width t] returns the number of columns in the dataframe [t]. *)
val width : t -> int

(** [shape t] returns the shape (rows, columns) of the dataframe [t]. *)
val shape : t -> int * int

(** [is_empty t] checks if the dataframe [t] is empty. *)
val is_empty : t -> bool

(** [sub t pos len] returns a sub dataframe starting from [pos] with length [len]. *)
val sub : t -> pos:int -> len:int -> t

(** [get_exn t col idx] gets the value in column [col] at row [idx] as an [any_value]
    option. *)
val get_exn : t -> col:int -> idx:int -> any_value

(** [get_row_exn t idx] gets the row at index [idx] as an array of [any_value]. *)
val get_row_exn : t -> idx:int -> any_value array

(** [get_by_name t col idx] gets the value in column [col] at row [idx] as an
    [any_value] option using the column name. *)
val get_by_name_exn : t -> col:string -> idx:int -> any_value

(** [sort_cols ?descending ?maintain_order df ~cols] sorts dataframe by columns [cols] and
    returns sorted dataframe. *)
val sort_cols : ?descending:bool -> ?maintain_order:bool -> t -> cols:string array -> t

(** [sort ?descending ?maintain_order df ~col] sorts dataframe by column [col] and returns
    sorted dataframe. *)
val sort : ?descending:bool -> ?maintain_order:bool -> t -> col:string -> t

(** [sort_cols_in_place ?descending ?maintain_order df ~cols] sorts dataframe by columns
    [cols] in place. *)
val sort_cols_in_place
  :  ?descending:bool
  -> ?maintain_order:bool
  -> t
  -> cols:string array
  -> unit

(** [sort_in_place ?descending ?maintain_order df ~cols] sorts dataframe by column [col]
    in place. *)
val sort_in_place : ?descending:bool -> ?maintain_order:bool -> t -> col:string -> unit

module Duration : sig
  type t = duration [@@deriving sexp]

  val zero : t
  val of_int_ms : int -> t
  val of_int_sec : int -> t
end

(** [upsample ?maintain_order ?by ~time_column ~every ~offset] upsample a dataframe at a regular time frequency. *)
val upsample
  :  ?maintain_order:bool
  -> ?by:string array
  -> ?offset:Duration.t
  -> t
  -> time_column:string
  -> every:Duration.t
  -> t

(** [rename_in_place tf ~col ~name] renames column [col] as [name] in place. *)
val rename_in_place : t -> col:string -> name:string -> unit

(** [column_names t cols] returns column names in this dataframe. *)
val column_names : t -> string array

(** [select t cols] selects columns from a dataframe. *)
val select : t -> string array -> t

(** [drop t cols] drops columns from a dataframe. *)
val drop : t -> string array -> t

(** [select_by_idx t col] selects a column by index [col] as a Series.t option. *)
val select_by_idx : t -> col:int -> Series.t option

(** [select_by_name t col] selects a column by name [col] as a Series.t option. *)
val select_by_name : t -> col:string -> Series.t option

(** [select_by_idx_exn t col] selects a column by index [col] as a Series.t. *)
val select_by_idx_exn : t -> col:int -> Series.t

(** [select_by_name_exn t col] selects a column by name [col] as a Series.t. *)
val select_by_name_exn : t -> col:string -> Series.t

(** [series_bigarrays_exn t] returns an array of (name, Bigarray) pairs representing the
    dataframe [t]. It raises an exception if series is not rechunked. *)
val series_bigarrays_exn : t -> (string * Any_bigarray.t) array

(** [null_counts t] counts null values in all series in this dataframe. *)
val null_counts : t -> int array

(** [null_count t] returns a sum of count of null values in all series in this dataframe. *)
val null_count : t -> int

(** [cast t ~col ~dtype] casts [col] series to [dtype]. *)
val cast : t -> col:string -> dtype:data_type -> t

(** [with_column t ~col] returns a dataframe with column [col]. *)
val with_column : t -> col:Series.t -> t

(** [read_csv ?skip_rows ?has_header ?columns ?schema ?threads file_path] reads a CSV file
    at [file_path] into a dataframe. *)
val read_csv
  :  ?skip_rows:int
  -> ?has_header:bool
  -> ?columns:string array
  -> ?schema:Schema.t
  -> ?threads:int
  -> string
  -> t

(** [read_csv' ?skip_rows ?has_header ?columns ?schema ?threads file_path] asynchronously
    reads a CSV file at [file_path] into a dataframe. *)
val read_csv'
  :  ?skip_rows:int
  -> ?has_header:bool
  -> ?columns:string array
  -> ?schema:Schema.t
  -> ?threads:int
  -> string
  -> t Deferred.t

(** [write_parquet t file_path] writes the dataframe [t] to a Parquet file at [file_path]. *)
val write_parquet : t -> string -> int

(** [write_parquet' t file_path] asynchronously writes the dataframe [t] to a Parquet file
    at [file_path]. *)
val write_parquet' : t -> string -> int Deferred.t

(** [read_parquet ?rechunk ?parallel file_path] reads a Parquet file at [file_path] into a
    dataframe. *)
val read_parquet : ?rechunk:bool -> ?parallel:bool -> string -> t

(** [read_parquet' ?rechunk ?parallel file_path] asynchronously reads a Parquet file at
    [file_path] into a dataframe. *)
val read_parquet' : ?rechunk:bool -> ?parallel:bool -> string -> t Deferred.t

(** [filter_col_by_name t ~col cmp value] filters the dataframe [t] by applying the
    comparison [cmp] to the column [col] and [value]. *)
val filter_col_by_name : t -> col:string -> comparison -> any_value -> t

(** [filter_col_by_name_multi t filters] filters the dataframe [t] using multiple filter
    conditions specified in [filters]. *)
val filter_col_by_name_multi : t -> (string * comparison * any_value) array -> t
